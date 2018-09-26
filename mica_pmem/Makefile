all:
	g++ -g test.cc -o test -lpmem -lcityhash -lpthread -lgtest -lnuma
	g++ -g -O3 -DNDEBUG bench.cc -o bench -lpmem -lcityhash -lpthread -lgtest -lnuma -lgflags -march=native
clean:
	rm test
