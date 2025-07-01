# Makefile optimizado para búsquedas rápidas
CC = gcc
CFLAGS = -Wall -Wextra -pedantic -std=c11 -O3
LDFLAGS = -lrt

TARGETS = preprocess search_server client

all: $(TARGETS)

preprocess: preprocess.c common.h
	$(CC) $(CFLAGS) -o $@ $<
	
search_server: search_server.c common.h
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)
	
client: client.c common.h
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGETS) *.o
	rm -f data.bin slot_index.bin metadata.bin hashtable.bin
	rm -f /tmp/search_request /tmp/search_response_*

preprocess-data: preprocess
	./preprocess dataset.csv

run-server: search_server
	./search_server

run-client: client
	./client

.PHONY: all clean preprocess-data run-server run-client