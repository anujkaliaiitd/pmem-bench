DPDK_HOME=/usr

CFLAGS=-Wall -Wextra -Werror -pedantic -fpermissive -march=native \
			 -Wold-style-cast -Wsign-conversion \
			 -Wno-unused-function

all:
	g++ -O3 -std=c++11 ${CFLAGS} -o bench bench.cc -isystem ${DPDK_HOME}/include/dpdk/ -march=native -L ${DPDK_HOME}/lib/ \
  -Wl,--whole-archive \
  -ldpdk -lnuma -lpthread -ldl -lm -lgflags -lpmem \
  -Wl,--no-whole-archive \

clean:
	rm bench
