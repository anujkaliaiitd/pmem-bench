make
rm -f /tmp/hogout
sudo -E taskset -c 23 ./hog > /tmp/hogout &
