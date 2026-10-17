//! What wakes the inventory: kernel uevents of block devices and changes
//! of the host mount table, read by one thread that polls both and an
//! eventfd that ends it.

use std::os::fd::OwnedFd;
use std::path::Path;

use model::DiskWatch;
use rustix::event::{EventfdFlags, PollFd, PollFlags, eventfd, poll};
use rustix::io::Errno;
use rustix::net::netlink::SocketAddrNetlink;
use rustix::net::{RecvFlags, recvfrom};
use tokio::sync::mpsc::UnboundedSender;

use super::Wake;

/// The kernel's uevent group; udev's own rebroadcast is group 2.
const KERNEL_GROUP: u32 = 1;
/// Room for a burst (hotplug of many disks) before the kernel drops events.
const RECV_BUFFER: usize = 4 << 20;

/// The wake a kernel uevent gives, if it is about a block device.
/// The message is `action@devpath` then `KEY=value` fields, NUL separated.
pub fn parse_uevent(msg: &[u8]) -> Option<Wake> {
    let mut fields = msg.split(|b| *b == 0).map(String::from_utf8_lossy);
    let head = fields.next()?;
    if !head.contains('@') {
        return None;
    }
    let (mut subsystem, mut devtype, mut devname, mut devpath) = (None, None, None, None);
    for f in fields {
        if let Some((k, v)) = f.split_once('=') {
            match k {
                "SUBSYSTEM" => subsystem = Some(v.to_owned()),
                "DEVTYPE" => devtype = Some(v.to_owned()),
                "DEVNAME" => devname = Some(v.trim_start_matches("/dev/").to_owned()),
                "DEVPATH" => devpath = Some(v.to_owned()),
                _ => {}
            }
        }
    }
    if subsystem.as_deref() != Some("block") {
        return None;
    }
    match devtype.as_deref() {
        Some("disk") => Some(devname.map_or(Wake::Refresh, Wake::Disk)),
        // A partition's disk: its partition list and signatures may change.
        Some("partition") => {
            let parent = devpath.as_deref().and_then(|p| p.rsplit('/').nth(1));
            Some(parent.map_or(Wake::Refresh, |d| Wake::Disk(d.to_owned())))
        }
        _ => Some(Wake::Refresh),
    }
}

fn uevent_socket() -> std::io::Result<OwnedFd> {
    use rustix::net::netlink::KOBJECT_UEVENT;
    use rustix::net::{AddressFamily, SocketFlags, SocketType, bind, socket_with, sockopt};
    let fd = socket_with(
        AddressFamily::NETLINK,
        SocketType::DGRAM,
        SocketFlags::CLOEXEC | SocketFlags::NONBLOCK,
        Some(KOBJECT_UEVENT),
    )?;
    // The forced size needs CAP_NET_ADMIN; the plain one is capped by rmem_max.
    if sockopt::set_socket_recv_buffer_size_force(&fd, RECV_BUFFER).is_err() {
        let _ = sockopt::set_socket_recv_buffer_size(&fd, RECV_BUFFER);
    }
    bind(&fd, &SocketAddrNetlink::new(0, KERNEL_GROUP))?;
    Ok(fd)
}

/// The thread's end that is not a shutdown: the loop polls from now on.
fn lost(tx: &UnboundedSender<Wake>, why: &str) {
    tracing::warn!(error = why, "uevent watch ended");
    let _ = tx.send(Wake::WatchLost);
}

/// Reads every queued uevent; false when the thread must end.
fn drain(sock: &OwnedFd, buf: &mut [u8], tx: &UnboundedSender<Wake>) -> bool {
    loop {
        match recvfrom(sock, &mut *buf, RecvFlags::empty()) {
            Ok((n, _, from)) => {
                // Only the kernel sends as pid 0; udevd and libudev users
                // send under their own pid.
                let pid = from
                    .and_then(|a| SocketAddrNetlink::try_from(a).ok())
                    .map_or(0, |a| a.pid());
                if pid != 0 {
                    continue;
                }
                if let Some(w) = parse_uevent(&buf[..n])
                    && tx.send(w).is_err()
                {
                    return false;
                }
            }
            Err(Errno::AGAIN) => return true,
            Err(Errno::INTR) => {}
            // The kernel dropped events: read everything again.
            Err(Errno::NOBUFS) => {
                if tx.send(Wake::All).is_err() {
                    return false;
                }
            }
            Err(e) => {
                lost(tx, &format!("recvfrom: {e}"));
                return false;
            }
        }
    }
}

/// The kernel flags a mounts file with POLLPRI and POLLERR when the
/// namespace's mount table changed since the last poll of it.
fn watch(sock: &OwnedFd, table: &std::fs::File, stop: &OwnedFd, tx: &UnboundedSender<Wake>) {
    let mut buf = vec![0u8; 64 << 10];
    loop {
        let mut fds = [
            PollFd::new(sock, PollFlags::IN),
            PollFd::new(table, PollFlags::PRI),
            PollFd::new(stop, PollFlags::IN),
        ];
        match poll(&mut fds, None) {
            Ok(_) => {}
            Err(Errno::INTR) => continue,
            Err(e) => {
                lost(tx, &format!("poll: {e}"));
                return;
            }
        }
        let [s, t, q] = fds.map(|f| f.revents());
        if !q.is_empty() {
            return;
        }
        if t.intersects(PollFlags::PRI | PollFlags::ERR) && tx.send(Wake::Refresh).is_err() {
            return;
        }
        if s.intersects(PollFlags::ERR | PollFlags::HUP | PollFlags::NVAL) {
            lost(tx, "socket error");
            return;
        }
        if s.intersects(PollFlags::IN) && !drain(sock, &mut buf, tx) {
            return;
        }
    }
}

/// The watcher thread; dropping it ends the thread.
#[derive(Debug)]
pub struct Watcher {
    stop: OwnedFd,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for Watcher {
    fn drop(&mut self) {
        let _ = rustix::io::write(&self.stop, &1u64.to_ne_bytes());
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn spawn(tx: &UnboundedSender<Wake>, mountinfo: &Path) -> std::io::Result<Watcher> {
    let sock = uevent_socket()?;
    let table = std::fs::File::open(mountinfo)?;
    let stop = eventfd(0, EventfdFlags::CLOEXEC | EventfdFlags::NONBLOCK)?;
    let mine = stop.try_clone()?;
    let tx = tx.clone();
    let thread = std::thread::Builder::new()
        .name("disk-watch".into())
        .spawn(move || watch(&sock, &table, &stop, &tx))?;
    Ok(Watcher {
        stop: mine,
        thread: Some(thread),
    })
}

/// Starts the watch; `Poll` and no watcher when it cannot run.
pub fn start(tx: &UnboundedSender<Wake>, mountinfo: &Path) -> (DiskWatch, Option<Watcher>) {
    match spawn(tx, mountinfo) {
        Ok(w) => (DiskWatch::Uevent, Some(w)),
        Err(error) => {
            tracing::warn!(%error, "no uevent or mount table watch: NodeDisks checks every disk every 5 minutes");
            (DiskWatch::Poll, None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// As the kernel sent it on a test VM after mkfs.xfs on a disk: udev's
    /// synthetic change for a close after write.
    const MKFS_CHANGE: &[u8] = b"change@/devices/pci0000:00/0000:00:02.3/0000:04:00.0/virtio9/block/vdf\0ACTION=change\0DEVPATH=/devices/pci0000:00/0000:00:02.3/0000:04:00.0/virtio9/block/vdf\0SUBSYSTEM=block\0SYNTH_UUID=0\0DEVNAME=vdf\0DEVTYPE=disk\0DISKSEQ=154\0SEQNUM=12778\0MAJOR=252\0MINOR=80\0";

    #[test]
    fn block_uevents() {
        assert_eq!(parse_uevent(MKFS_CHANGE), Some(Wake::Disk("vdf".into())));
        let part = b"add@/devices/virtual/x/block/sdb/sdb1\0ACTION=add\0DEVPATH=/devices/virtual/x/block/sdb/sdb1\0SUBSYSTEM=block\0DEVNAME=sdb1\0DEVTYPE=partition\0";
        assert_eq!(parse_uevent(part), Some(Wake::Disk("sdb".into())));
        let dm = b"add@/devices/virtual/block/dm-4\0ACTION=add\0SUBSYSTEM=block\0DEVNAME=dm-4\0DEVTYPE=disk\0";
        assert_eq!(parse_uevent(dm), Some(Wake::Disk("dm-4".into())));
        let net = b"add@/devices/virtual/net/veth0\0ACTION=add\0SUBSYSTEM=net\0";
        assert_eq!(parse_uevent(net), None);
        // udev's own messages start with "libudev", never on group 1.
        assert_eq!(parse_uevent(b"libudev\0\xfe\xed"), None);
    }

    /// The thread ends on drop; where the socket cannot open, start
    /// says so instead of failing.
    #[test]
    fn watcher_stops_on_drop() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (watch, w) = start(&tx, Path::new("/proc/self/mountinfo"));
        match watch {
            DiskWatch::Uevent => {
                let w = w.expect("watcher");
                let t0 = std::time::Instant::now();
                drop(w);
                assert!(t0.elapsed() < std::time::Duration::from_secs(2));
                // A shutdown is not a lost watch.
                assert!(!matches!(rx.try_recv(), Ok(Wake::WatchLost)));
            }
            DiskWatch::Poll => assert!(w.is_none()),
        }
    }
}
