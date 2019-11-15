#!/usr/bin/env bash
dpdk=~/sandbox/dpdk-19.08/

sudo modprobe uio
sudo insmod $dpdk/x86_64-native-linux-gcc/kmod/igb_uio.ko

#sudo dpdk-devbind --bind=igb_uio "$ifname"

# Create hugepage mount
sudo mkdir -p /mnt/huge
grep -s /mnt/huge /proc/mounts > /dev/null

if [ $? -ne 0 ] ; then
  sudo mount -t hugetlbfs nodev /mnt/huge
fi

# Bind IOAT devices
for i in `seq 0 7`; do
  sudo ${dpdk}/usertools/dpdk-devbind.py -b igb_uio 0000:00:04.$i
done
