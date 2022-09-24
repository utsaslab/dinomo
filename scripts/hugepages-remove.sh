hugeadm --pool-pages-min 2MB:0
umount /mnt/hugetlbfs
cat /sys/devices/system/node/node*/meminfo | fgrep Huge

