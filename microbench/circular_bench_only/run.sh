exe="./main"
chmod +x $exe

num_threads=1

if [ "$#" -gt 1 ]; then
  blue "Illegal number of arguments."
  blue "Usage: ./run.sh, or ./run.sh gdb"
	exit
fi

# Check for non-gdb mode
if [ "$#" -eq 0 ]; then
  numactl --physcpubind=3 --membind=0 $exe --num_threads=$num_threads
fi

# Check for gdb mode
if [ "$#" -eq 1 ]; then
  gdb -ex run --args $exe --num_threads=$num_threads
fi
