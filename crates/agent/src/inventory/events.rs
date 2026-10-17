//! What wakes the inventory: kernel uevents of block devices and changes
//! of the host mount table.

use std::os::fd::OwnedFd;
use std::path::Path;

use model::DiskWatch;
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;
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
    use rustix::net::netlink::{KOBJECT_UEVENT, SocketAddrNetlink};
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

async fn uevents(fd: AsyncFd<OwnedFd>, tx: UnboundedSender<Wake>) {
    let mut buf = vec![0u8; 64 << 10];
    loop {
        let Ok(mut guard) = fd.readable().await else {
            tracing::warn!("uevent socket failed: disk changes show at the next full check");
            return;
        };
        loop {
            match rustix::net::recv(fd.get_ref(), &mut buf[..], rustix::net::RecvFlags::empty()) {
                Ok((n, _)) => {
                    if let Some(w) = parse_uevent(&buf[..n])
                        && tx.send(w).is_err()
                    {
                        return;
                    }
                }
                Err(rustix::io::Errno::AGAIN) => {
                    guard.clear_ready();
                    break;
                }
                // The kernel dropped events: read everything again.
                Err(rustix::io::Errno::NOBUFS) => {
                    if tx.send(Wake::All).is_err() {
                        return;
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "uevent receive failed");
                    guard.clear_ready();
                    break;
                }
            }
        }
    }
}

/// The kernel flags a mounts file with POLLPRI when the namespace's mount
/// table changes; the next poll clears it.
async fn mounts(fd: AsyncFd<std::fs::File>, tx: UnboundedSender<Wake>) {
    loop {
        let Ok(mut guard) = fd.ready(Interest::PRIORITY).await else {
            tracing::warn!("mount table watch failed: mounts show at the next full check");
            return;
        };
        guard.clear_ready();
        if tx.send(Wake::Refresh).is_err() {
            return;
        }
    }
}

/// Starts the watches; `Poll` when either cannot run.
// The safe AsyncFd constructors are deprecated only because a caller could
// close the fd while registered; these own theirs (OwnedFd, File) and keep
// it, and the crate forbids unsafe.
#[allow(deprecated)]
pub fn start(tx: &UnboundedSender<Wake>, mountinfo: &Path) -> DiskWatch {
    let socket = uevent_socket().and_then(AsyncFd::new);
    let table =
        std::fs::File::open(mountinfo).and_then(|f| AsyncFd::with_interest(f, Interest::PRIORITY));
    match (socket, table) {
        (Ok(s), Ok(t)) => {
            tokio::spawn(uevents(s, tx.clone()));
            tokio::spawn(mounts(t, tx.clone()));
            DiskWatch::Uevent
        }
        (s, t) => {
            let error = s
                .err()
                .or(t.err())
                .map(|e| e.to_string())
                .unwrap_or_default();
            tracing::warn!(%error, "no uevent or mount table watch: NodeDisks checks every disk every 5 minutes");
            DiskWatch::Poll
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// As the kernel sent it on vm3 after mkfs.xfs on a test disk: udev's
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
}
