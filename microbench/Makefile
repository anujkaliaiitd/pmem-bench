CPP_FLAGS=-Wall -Wextra -Werror -pedantic -Wsign-conversion -Wold-style-cast -Wno-unused-function -march=native
SOURCES=bench.cc
LIBS=-libverbs -lgtest -lpthread -lmemcached -lgflags -lnuma -lpmem

all:
	g++ -std=c++11 -O3 ${CPP_FLAGS} -o bench ${SOURCES} ${LIBS}

clean:
	rm bench
