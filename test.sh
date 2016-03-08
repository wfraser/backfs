#!/bin/bash

# Not sure how portable this is...
thisScript=$(readlink /proc/$$/fd/255)
backfsDir=$(dirname $thisScript)
cd $backfsDir
backfs=$backfsDir/backfs

sudo umount test/cachefs

rm -rf test
mkdir test
cd test
mkdir cachefs
mkdir backing_store
mkdir mount

# Copy our source code to use as the backing store.
cp -v $backfsDir/*.c backing_store
cp -v $backfsDir/*.h backing_store

dd if=/dev/zero of=cachefs.img bs=10M count=1
mkfs.ext2 -m 0 -F cachefs.img
sudo mount -o loop cachefs.img cachefs
sudo chown -R `whoami` cachefs

valgrind --leak-check=full $backfs -d -o cache=cachefs,rw backing_store mount

# do stuff...

#sudo umount cachefs
