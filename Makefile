CFLAGS=-Wall -g3 -D_FILE_OFFSET_BITS=64 -DFUSE_USE_VERSION=28 -I/usr/include/fuse

LDFLAGS=-lfuse

all: backfs

backfs: backfs.o fscache.o fsll.o

clean:
	rm -f *.o backfs

# vim: noet sts=8 ts=8 sw=8
