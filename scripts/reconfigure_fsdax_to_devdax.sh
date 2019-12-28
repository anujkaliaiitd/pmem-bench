# Destroy fsdax namespaces

# This can fail even when /mnt/pmem0 and /mnt/pmem1 are empty. Rebooting solves it.
echo "Unmounting"
sudo umount /mnt/pmem0
sudo umount /mnt/pmem1

echo "Destroying namespaces via ndctl. This takes a while."
sudo ndctl destroy-namespace -f all

echo "Recreating devdax namespaces"
sudo ndctl create-namespace --mode devdax --region 0
sudo ndctl create-namespace --mode devdax --region 1
