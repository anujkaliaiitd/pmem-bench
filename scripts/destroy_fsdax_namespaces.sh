# Destroy fsdax namespaces

echo "Unmounting"
sudo umount /mnt/pmem0
sudo umount /mnt/pmem1

echo "Destroying namespaces via ndctl. This takes a while."
sudo ndctl destroy-namespace -f all
