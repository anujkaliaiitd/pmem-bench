exe="../build/microbench"
chmod +x $exe

num_threads=1
numactl --cpunodebind=0 --membind=0 $exe --num_threads=$num_threads
