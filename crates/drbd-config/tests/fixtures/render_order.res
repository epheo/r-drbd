resource "pvc-0a1b"
{

    handlers
    {
        quorum-lost "/bin/true";
    }

    options
    {
        on-no-data-accessible suspend-io;
        on-no-quorum suspend-io;
        on-suspended-primary-outdated force-secondary;
        quorum majority;
    }

    net
    {
        cram-hmac-alg     sha1;
        shared-secret     "q2XlS0v3Yp9cJr7nAa1F";
        protocol C;
        rr-conflict retry-connect;
    }

    disk
    {
        al-extents 6007;
    }

    on "node-a"
    {
        volume 0
        {
            disk        /dev/thinpool/pvc-0a1b_00000;
            tiebreaker yes;
            disk
            {
                rs-discard-granularity 65536;
            }
            meta-disk   internal;
            device      minor 1000;
        }
        node-id    0;
    }

    on "node-b"
    {
        volume 0
        {
            # placeholder path: the peer's real backing device is never opened here
            disk        /dev/drbd/this/is/not/used;
            tiebreaker yes;
            disk
            {
                rs-discard-granularity 65536;
            }
            meta-disk   internal;
            device      minor 1000;
        }
        node-id    1;
    }

    on "node-c"
    {
        volume 0
        {
            disk        none;
            tiebreaker yes;
            disk
            {
                rs-discard-granularity 65536;
            }
            meta-disk   internal;
            device      minor 1000;
        }
        node-id    2;
    }

    connection
    {

        net
        {
            protocol C;
            rr-conflict retry-connect;
        }
        host "node-a" address ipv4 10.0.0.11:7000;
        host "node-b" address ipv4 10.0.0.12:7000;
    }

    connection
    {

        net
        {
            protocol C;
            rr-conflict retry-connect;
        }
        host "node-a" address ipv4 10.0.0.11:7000;
        host "node-c" address ipv4 10.0.0.13:7000;
    }
}
