# On the Intel AEP servers, SPDK is installed at system-level, but DPDK isn't.
all:
	gcc -o hello hello.c -L /home/akalia/sandbox/spdk/dpdk/build/lib \
	-lspdk_nvme -lspdk_util -lspdk_env_dpdk -lspdk_log \
	-lpthread -ldpdk -lnuma -ldl -luuid
