CC ?= gcc
AR ?= ar
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=gnu11 -D_GNU_SOURCE -fPIC
INCLUDES := -I../../pkg/xds -I../../xDS/kernels/include -I../../xDS/kernels/user/include
P2P_SRC := ../../xDS/kernels/user/xds_p2p_user.c

.PHONY: all clean

all: libjfs_xds.so libjfs_xds.a

jfs_xds.o: jfs_xds.c jfs_xds.h ../../pkg/xds/proto.h
	$(CC) $(CFLAGS) $(INCLUDES) -c jfs_xds.c -o $@

xds_p2p_user.o: $(P2P_SRC)
	$(CC) $(CFLAGS) $(INCLUDES) -Wno-unused-parameter -c $(P2P_SRC) -o $@

libjfs_xds.so: jfs_xds.o xds_p2p_user.o
	$(CC) -shared -o $@ $^ -lpthread

libjfs_xds.a: jfs_xds.o xds_p2p_user.o
	$(AR) rcs $@ $^

clean:
	rm -f jfs_xds.o xds_p2p_user.o libjfs_xds.so libjfs_xds.a
