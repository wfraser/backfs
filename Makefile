PREFIX=/usr/local

DEFINES=-D_FILE_OFFSET_BITS=64 -DFUSE_USE_VERSION=28 -D_POSIX_C_SOURCE=201201

CFLAGS=-std=c1x -pedantic -g3 $(DEFINES) -I/usr/include/fuse -DBACKFS_VERSION="\"$(VERSION)\""

VERSION=BackFS v0.2\
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
