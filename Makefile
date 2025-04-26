
W	= -W -Wall -Wno-unused-parameter -Wbad-function-cast -Wuninitialized
THREADS = -pthread
OPT     = -Og -g
INCS    = -I/usr/local/include/luajit-2.1 -I./liblmdb
LIBS    = -L/usr/local/lib -lluajit-5.1
CFLAGS	= $(THREADS) $(OPT) $(W) $(XCFLAGS) $(INCS)

.PHONY: all clean doc

all: lmdb.so

lmdb.so: lmdb.c liblmdb/mdb.c liblmdb/midl.c
	$(CC) -shared $(CFLAGS) -o $@ $^ $(LIBS)

test:  lmdb.so test.lua
	luajit test.lua

doc:
	ldoc -c config.ld lmdb.c

clean:
	rm -rf lmdb.so* *.o doc

