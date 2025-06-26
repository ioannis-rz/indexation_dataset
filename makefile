CC = gcc
CFLAGS = -Wall -Wextra -O3
TARGETS = preprocess search_server client

all: $(TARGETS)

preprocess: preprocess.c
	$(CC) $(CFLAGS) -o $@ $^

search_server: search_server.c
	$(CC) $(CFLAGS) -o $@ $^

client: client.c
	$(CC) $(CFLAGS) -o $@ $^

clean:
	rm -f $(TARGETS) *.bin
	rm -f /tmp/search_*

.PHONY: all clean