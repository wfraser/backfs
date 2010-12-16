#!/bin/bash

case "$1" in
    on)
        echo "mounting backfs cache..."
        mount -o loop /var/cache/backfs.img /var/cache/backfs
        if [ $? != 0 ]; then 
            echo "fail!"
            exit 1
        fi
        echo "ok"

        echo "mounting sshfs..."
        sshfs -o uid=0,gid=1000,umask=007 wfraser@smokey.codewise.org:/home/store /mnt/smokey-store
        if [ $? != 0 ]; then 
            echo "fail!"
            umount /var/cache/backfs
            exit 2
        fi
        echo "ok"

        echo "mounting backfs..."
        #backfs -o cache=/var/cache/backfs /mnt/smokey-store /mnt/store
        echo "(not really)"
        if [ $? != 0 ]; then
            echo "fail!";
            umount /var/cache/backfs
            umount /mnt/smokey-store
            exit 3
        fi
        ;;

    off)
        echo "umounting..."
        umount /mnt/store
        umount /var/cache/backfs
        umount /mnt/smokey-store
        echo "ok"
        ;;
    
    *)
        echo "backfs.sh (on|off)"
        exit -1
esac

