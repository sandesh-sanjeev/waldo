## RLIMIT_MEMLOCK

This crate uses io-uring to drive async I/O operations. Registering buffers with io-uring allows for better
performance by allowing the kernel to hold long term references to shared memory. Unfortunately this also
counts against users memlock limit. Users who wish to take advantage of this, and users are encouraged to, 
should increase memlock limits appropriately.

On ArchLinux, default was set at 8 MB. After much struggle, I figured out that I had to use systemd to configure 
limits for my user (`/etc/systemd/user.conf`). You mileage may vary.
