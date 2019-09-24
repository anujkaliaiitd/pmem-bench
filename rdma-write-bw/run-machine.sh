#!/usr/bin/env bash
source $(dirname $0)/../scripts/utils.sh
source $(dirname $0)/../scripts/mlx_env.sh
#export HRD_REGISTRY_IP="fawn-pluto0"
#export HRD_REGISTRY_IP="akalianode-1.rdma.fawn.apt.emulab.net"
export HRD_REGISTRY_IP="192.168.18.2"

# Check number of arguments
if [ "$#" -gt 2 ]; then
  blue "Illegal number of arguments."
  blue "Usage: ./run-machine.sh <machine_id>, or ./run-machine.sh <machine_id> gdb"
	exit
fi

if [ "$#" -eq 0 ]; then
  blue "Illegal number of arguments."
  blue "Usage: ./run-machine.sh <machine_id>, or ./run-machine.sh <machine_id> gdb"
	exit
fi

machine_id=$1
num_threads=24

drop_shm
exe="./write-bw"
chmod +x $exe

# Check for non-gdb mode
if [ "$#" -eq 1 ]; then
  sudo -E numactl --physcpubind=0 --membind=0 $exe --is_client 1 \
    --machine_id $machine_id --num_threads $num_threads
fi

# Check for gdb mode
if [ "$#" -eq 2 ]; then
  sudo -E gdb -ex run --args $exe --is_client 1 \
    --machine_id $machine_id --num_threads $num_threads
fi
