CFLAGS=-g -Wpadded -Werror -Wall -O3
CC=clang
LDLIBS=

softheap.o: softheap.c
	$(CC) $(CFLAGS) $(LDFLAGS) -c softheap.c

clean:
	rm *.o

.PHONY: clean
