CPP_FLAGS=-Wall -Wextra -Werror -pedantic -Wsign-conversion -Wold-style-cast -Wno-unused-function -march=native
SOURCES=../libhrd_cpp/hrd_conn.cc ../libhrd_cpp/hrd_util.cc main.cc
LIBS=-libverbs -lgtest -lpthread -lmemcached -lgflags -lnuma -lpmem

all:
	g++ -std=c++11 -O3 ${CPP_FLAGS} -o write-bw ${SOURCES} ${LIBS}

clean:
	rm write-bw
