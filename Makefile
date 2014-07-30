CFLAGS=-g -Wall -O3
CC=clang
LDLIBS=

softheap.so: softheap.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $*.c
