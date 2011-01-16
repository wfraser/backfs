CFLAGS=-Wall -g3 -D_FILE_OFFSET_BITS=64 -DFUSE_USE_VERSION=28 -I/usr/include/fuse -DSYSLOG -DCOMPILEDATE=$(VERSION)

VERSION="\"BackFS v0.0\ngit version $(shell git log --pretty="format:%h %ai" -n1)\nbuilt $(shell date "+%Y-%m-%d %H:%M:%S %z")\""

LDFLAGS=-lfuse

all: backfs

backfs: backfs.o fscache.o fsll.o

clean:
	rm -f *.o backfs

# vim: noet sts=8 ts=8 sw=8
