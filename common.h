#ifndef COMMON_H
#define COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <stdint.h>
#include <errno.h>
#include <signal.h>
#include <sys/mman.h>

#define DATA_FILE "data.bin"
#define SLOT_INDEX_FILE "slot_index.bin"
#define METADATA_FILE "metadata.bin"
#define MAX_MEMORY 10 * 1024 * 1024
#define REQUEST_PIPE "/tmp/search_request"
#define RESPONSE_PIPE_TEMPLATE "/tmp/search_response_%d"
#define HASH_SIZE 1000003  // Tamaño primo para la tabla hash

// Tipos de búsqueda
typedef enum {
    SEARCH_BY_SLOT = 1,
    SEARCH_BY_TX_IDX,
    SEARCH_BY_DIRECTION,
    SEARCH_BY_WALLET,
    SEARCH_BY_ROW
} SearchType;

// Estructura de registro
typedef struct {
    char block_time[20];
    unsigned int slot;
    unsigned int tx_idx;
    char signing_wallet[50];
    char direction[5];
    char base_coin[100];
    unsigned long long base_coin_amount;
    unsigned long long quote_coin_amount;
    unsigned long long virtual_token_balance_after;
    unsigned long long virtual_sol_balance_after;
    char signature[100];
    unsigned long provided_gas_fee;
    unsigned long provided_gas_limit;
    unsigned long fee;
    unsigned long consumed_gas;
} Record;

// Índice de bloques
typedef struct {
    unsigned int min_slot;
    unsigned int max_slot;
    long offset;
} BlockIndex;

// Metadatos
typedef struct {
    unsigned int record_count;
    unsigned int block_count;
    size_t record_size;
} Metadata;

// Solicitud de búsqueda combinada
typedef struct {
    int client_pid;
    SearchType type1;
    SearchType type2;
    union {
        unsigned int slot;
        unsigned int tx_idx;
        unsigned int row;
        char direction[5];
        char wallet[50];
    } param1;
    union {
        unsigned int slot;
        unsigned int tx_idx;
        unsigned int row;
        char direction[5];
        char wallet[50];
    } param2;
} SearchRequest;

// Añadir al final de common.h
typedef struct HashEntry {
    uint64_t key;           // slot << 32 | tx_idx
    long offset;            // Offset en data.bin
    struct HashEntry* next; // Puntero para encadenamiento
} HashEntry;

typedef struct {
    HashEntry** buckets;    // Array de buckets
    size_t size;            // Tamaño de la tabla
} HashTable;

unsigned int hash_function(uint64_t key);

#endif // COMMON_H