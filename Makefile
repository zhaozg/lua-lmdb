
W	= -W -Wall -Wno-unused-parameter -Wbad-function-cast -Wuninitialized
THREADS = -pthread
OPT     = -Og -g -fPIC
LUALIBS = $(shell pkg-config -libs luajit)
LUAINCS = $(shell pkg-config --cflags luajit)
INCS    = $(LUAINCS) -I./liblmdb
LIBS    = $(LUALIBS)
CFLAGS	= $(THREDS) $(OPT) $(W) $(XCFLAGS) $(INCS)

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

