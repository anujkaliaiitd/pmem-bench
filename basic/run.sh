num_threads=1
numactl --cpunodebind=0 --membind=0 ./bench --num_threads=$num_threads
