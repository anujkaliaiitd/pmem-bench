all:
	g++ -g -o test test.cc -lcityhash -lgtest -lpmem
	g++ -g -O3 -DNDEBUG bench.cc -o bench -lpmem -lcityhash -lpthread -lgtest -lnuma -lgflags -march=native
clean:
	rm test bench
