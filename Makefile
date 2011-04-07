PREFIX=/usr/local

CFLAGS=-Wall -g3 -D_FILE_OFFSET_BITS=64 -DFUSE_USE_VERSION=28 -I/usr/include/fuse -DSYSLOG -DBACKFS_VERSION="\"$(VERSION)\""

VERSION=BackFS v0.0\
$(shell test -d .git && echo "\ngit revision" && git log --pretty="format:%h %ai" -n1)\
\nbuilt $(shell date "+%Y-%m-%d %H:%M:%S %z")\n

LDFLAGS=-lfuse

all: backfs

backfs: backfs.o fscache.o fsll.o

clean:
	rm -f *.o *~ backfs

install: backfs
	echo cp backfs $(PREFIX)/bin
	echo chmod 0755 $(PREFIX)/bin/backfs

# vim: noet sts=8 ts=8 sw=8
