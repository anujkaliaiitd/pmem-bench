batch_size=16
benchmark=get
sweep_optimizations=1
pmem_file="/mnt/pmem12/raft_log"

one_million=1048576  # Just a constant to adjust keys_total below
keys_total=`expr 64 \* $one_million`

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
      --sweep_optimizations $sweep_optimizations \
      --num_threads $num_threads
  fi
  printf "\n\n"
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
