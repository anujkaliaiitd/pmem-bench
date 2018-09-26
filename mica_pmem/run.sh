batch_size=8
num_threads=8
benchmark=get

keys_total=1000000000

for num_threads in 1; do
  keys_per_thread=`expr $keys_total / $num_threads`

  # Non-GDB mode
  if [ "$#" -eq 0 ]; then
    numactl --cpunodebind=0 --membind=0 ./bench \
      --table_key_capacity $keys_per_thread \
      --batch_size $batch_size \
      --benchmark $benchmark \
      --num_threads $num_threads
  fi
done


# GDB mode
if [ "$#" -eq 1 ]; then
  echo "do.sh: Launching process with GDB"
  num_keys=65536
  gdb -ex run --args ./bench \
      --table_key_capacity $num_keys \
      --batch_size $batch_size \
      --benchmark $benchmark \
      --num_threads $num_threads
fi
