exe="./bench"
chmod +x $exe

if [ "$#" -gt 2 ]; then
  blue "Illegal number of arguments."
  blue "Usage: ./run.sh <num_counters>, or ./run.sh <num_counters> <gdb>"
	exit
fi

num_counters=$1

# Check for non-gdb mode
if [ "$#" -eq 1 ]; then
  sudo numactl --physcpubind=3 --membind=0 $exe $num_counters
fi

# Check for gdb mode
if [ "$#" -eq 2 ]; then
  gdb -ex run --args $exe $num_counters
fi
