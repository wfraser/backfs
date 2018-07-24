[![Build Status](https://travis-ci.org/wfraser/backfs.svg?branch=master)](https://travis-ci.org/wfraser/backfs)

BackFS
======

BackFS is a FUSE filesystem designed to provide a large local disk cache for a remote network filesystem.

Say you have a network filesystem, such as a SSH or FTP mounted share, and the connection to that share is rather slow.
Locally, you have a sizable amount of disk space to spare, but not enough to make a complete local copy of the remote share.

What you can do is stick BackFS between the two and substantially speed up your data accesses and reduce network traffic.
When data is requested, BackFS checks its cache, and if the requested data's not there, it fetches chunks of it over the network and stores them in the cache.
This continues until the cache is full, and then new additions to the cache will be made by pushing out the least recently accessed data to make room.
The result is that the most frequently used data will be in the local cache and won't need to be fetched from the network, but the whole share is still available.

If the data in the backing store is updated, when someone requests that data BackFS will notice the change and invalidate its cached data automatically.

#### !! EXPERIMENTAL WARNING !! ####

#### !! BackFS is experimental code and should not be used on production systems !! ####

BackFS has been found to be reliable in normal usage scenarios, but is not coded to be 100% fault tolerant.
Unexpected errors could cause data corruption! Caveat emptor.

Usage
-----
     backfs -o cache=<cache storage> <slow backing store> <mount point>

Quick Setup
-----------
First, figure out how much disk space you have to spare. Run `df -h` or similar.

Let's say we can spare 20 gigs. Make a filesystem to hold the cache:

    $ dd if=/dev/zero of=/var/cache/backfs.img bs=1 count=0 seek=$((20*1024*1024*1024))
      0+0 records in
      0+0 records out
      0 bytes (0 B) copied, 1.4545e-05 s, 0.0 kB/s
    $ mkfs.ext4 -m 0 /var/cache/backfs.img
      mke2fs 1.42.5 (29-Jul-2012)
      /var/cache/backfs.img is not a block special device.
      Proceed anyway? (y,n) y
      ... snip

Then, mount the cache, and mount BackFS:

    $ mkdir /var/cache/backfs
    $ mount -o loop /var/cache/backfs.img /var/cache/backfs
    $ backfs -o cache=/var/cache/backfs /mnt/slow_network_share /mnt/backfs
      cache size: 20.0 GiB
      block size: 1048576 bytes

And that's it! You can now access your slow network share from /mnt/backfs just like a normal filesystem.
The only restriction is that it's read-only, but once the cache gets populated after some time using it, it'll now be much faster.

To unmount:

    $ fusermount -u /mnt/backfs
    $ umount /var/cache/backfs

And don't hurry to delete the cache filesystem image file.
If you leave the cache intact, the next time you mount BackFS, the cache will already be populated with your frequently used data.

Options
-------
* `-o cache`
       - mandatory: mount point for the cache

* `-o cache_size`
       - optional: max size the cache should be allowed to grow to.
         If unspecified, the cache will grow to fill the device it's on.
         (You probably want to have the cache be a separate filesystem in this case.)

* `-o backing_fs`
       - optional: alternate way to specify the backing store

* `-o block_size`
       - optional: size (in bytes) of the blocks stored in the cache.
         A read resulting in a cache miss will fetch this amount from the backing store.
         If unspecified, the default is 1 MiB (1048576 bytes).

* `-o rw`
       - optional: enable read-write mode. By default, BackFS operates as a read-only filesystem.
         This option allows BackFS to function as a write-through cache.

Requirements
------------

* [FUSE](https://github.com/libfuse/libfuse) version 3 or greater. (libfuse 3.2.4 is the earliest tested version)
* An operating system that fuse3 supports.
 
Linux is the only operating system I've tested, but others might work; nothing in the implementation is particularly tied to Linux versus other UNIXes.

Installation
------------

    $ make
    $ make install

It's that simple. You can add `PREFIX=/some/where` to the `make install` line to have it installed somewhere other than the default /usr/local

Implementation Details
----------------------

BackFS simply intercepts `open`, `read`, `opendir`, `readdir`, and `attr` calls using FUSE and passes them through to whatever filesystem you specified as the backing store.
The magic is in the cache and what the `read` syscall does with it.

The cache is separated into two datastructures: a map and a pair of doubly-linked lists working as queues.
These data structures are stored entirely as filesystem structures on the cache FS, heavily (ab)using symbolic links.
This is what allows BackFS's cache to survive across remounts.


### Buckets: ###

Data is stored in the `/buckets` subdirectory of the cache.
Buckets are numbered `0` through (theoretically) `18446744073709551615` (that's 2^64-1).
Each bucket is a directory with a couple files in it:

- `data`
    - The cache data. Only present for used buckets.
- `parent`
    - Symlink to the parent in the map directory. Only present for used buckets.
- `next`
    - Symlink to the next bucket in the queue. Not present if the bucket is the tail.
- `prev`
    - Symlink to the previous bucket in the queue. Not present if the bucket is the head.

Buckets are kept in two queues: the used queue and the free queue.
The used queue head is designated by a symlink named `/buckets/head` and the tail is `/buckets/tail`.
This queue holds cache data.
The head of the used queue is the bucket which was most recently accessed for reading.
The tail is the least recently accessed, and the next candidate for deletion.
When a bucket is accessed, it is promoted to the head of the used queue by snipping it out, joining its neighbors, and inserting it as the head.

If, when adding data to the cache, the cache either hits its configured storage limit or the device runs out of space,
BackFS will go through the used queue (starting at the tail -- the least recently accessed bucket)
and free buckets to make space for new data, until enough space has been made.

When a bucket is freed, several things happen in sequence:

- its `data` file is deleted
- the `parent` symlink is followed, the map file it pointed to is deleted
- the `parent` symlink itself is deleted
- the bucket is removed from the tail used queue
- the bucket is added to the tail of the free queue.
    
When buckets are freed, the directories are not deleted, but saved in the free queue for re-use.
This prevents the bucket numbers from getting ridiculously high and having to potentially deal with wraparound.
The free queue head and tail are pointed by `/buckets/free_head` and `/buckets/free_tail`, respectively (these symlinks might not exist, in the case where no buckets are currently free).

When a new bucket needs to be filled, one is pulled off the head of the free queue, if any is available, otherwise the max bucket number is incremented and a new bucket is made.
The number of the next bucket to be made is kept in a file called `/buckets/next_bucket_number`.

### Map: ###

The other data structure is a map from filenames to buckets.
When data is added to the cache, a directory in `/map` is made with the same name as the file the data was from, path and all.
E.g. if `/mnt/backing_store/foo/bar` is accessed, the map directory will be `/var/cache/backfs/map/foo/bar`.

Inside each map directory are symlinks to buckets of the file's cached data.
For example, with the default block size of 1 MiB, the first megabyte of `/foo/bar` would be pointed to by a symlink named `/map/foo/bar/0`.
That might point to `/buckets/4227` or something.

Also inside the map directory is a file `mtime` which contains the Unix timestamp of the file's modification time. This is checked against the backing store on each read, and if there is a mismatch, the cache data is deleted and refreshed.

When buckets are freed to make room in the cache, the corresponding map symlinks are removed.
BackFS also checks if the last block of a file was removed, and then removes that file's map directory as well, and if possible, its parent's, and its parent's parent's, etc., keeping the map tree minimal.

Advanced Usage
--------------

These are really just hacks right now, but can be useful.

A mounted BackFS has two magic files in its root: `.backfs_control` and `.backfs_version`.

`.backfs_version` just contains the current version number and build information.

`.backfs_control` can be used to issue some commands to BackFS by writing to it:

* `invalidate /file/name`
    - removes all blocks of `/file/name` from the cache (path is relative to the backing store root). The next read will come from the backing store and refresh the cache.

* `free_orphans`
    - removes any cache buckets not linked to a file in the filename/block map.

A quick and dirty way to invalidate a whole directory (*be careful, no guarantees this won't break if BackFS is writing to the map directory at the same time!*):

    $ cd /var/cache/backfs/map
    $ rm -r some/dir
    $ echo -n 'free_orphans' >> /mnt/backfs/.backfs_control

Strictly speaking, the last step isn't needed, because once buckets aren't linked in the map, they'll fall off the cache as it continues to be filled.

*Of course, you can also invalidate cache data by changing the file modification time, using a command like `touch`.*

Todo List
---------

* ???

License
-------

BackFS copyright (c) 2010-2018 William R. Fraser

BackFS is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation; either version 2 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

