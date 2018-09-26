batch_size=16
num_threads=8
benchmark=set

# Non-GDB mode
if [ "$#" -eq 0 ]; then
  for ((num_keys = (64 * 1024 * 1024); num_keys <= (64 * 1024 * 1024); num_keys *= 2)); do
    numactl --physcpubind=3 --membind=0 ./bench \
      --table_key_capacity $num_keys \
      --batch_size $batch_size \
      --benchmark $benchmark \
      --num_threads $num_threads
  done
fi


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
