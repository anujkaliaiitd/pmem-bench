batch_size=16
benchmark=5050
pmem_file="/dev/dax12.0"

keys_total=1000000000

rm -rf /tmp/mica_bench*

for num_threads in 1 2 4 8 16 24; do
  keys_per_thread=`expr $keys_total / $num_threads`

  # Non-GDB mode
  if [ "$#" -eq 0 ]; then
    numactl --cpunodebind=0 --membind=0 ./bench \
      --table_key_capacity $keys_per_thread \
      --batch_size $batch_size \
      --benchmark $benchmark \
      --pmem_file $pmem_file \
      --num_threads $num_threads
  fi
  printf "\n\n\n\n"
done

num_threads=1
# GDB mode
if [ "$#" -eq 1 ]; then
  echo "do.sh: Launching process with GDB"
  num_keys=65536
  gdb -ex run --args ./bench \
      --table_key_capacity $num_keys \
      --batch_size $batch_size \
      --benchmark $benchmark \
      --pmem_file $pmem_file \
      --num_threads $num_threads
fi
