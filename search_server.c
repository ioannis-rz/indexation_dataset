#include "common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <fcntl.h>
#include <unistd.h>

Metadata meta;
BlockIndex *block_index = NULL;
FILE *data_file = NULL;
FILE *slot_file = NULL;

void cleanup(int sig) {
    printf("\nSignal %d received. Cleaning up...\n", sig);

    if (block_index) {
        free(block_index);
        block_index = NULL;
    }
    if (data_file) {
        fclose(data_file);
        data_file = NULL;
    }
    if (slot_file) {
        fclose(slot_file);
        slot_file = NULL;
    }

    unlink(REQUEST_PIPE);
    exit(0);
}

int matches_criteria(Record *record, SearchRequest *req) {
    if (req->type1 != 0) {
        switch (req->type1) {
            case SEARCH_BY_SLOT:
                if (record->slot != req->param1.slot) return 0;
                break;
            case SEARCH_BY_TX_IDX:
                if (record->tx_idx != req->param1.tx_idx) return 0;
                break;
            case SEARCH_BY_DIRECTION:
                if (strcmp(record->direction, req->param1.direction) != 0) return 0;
                break;
            case SEARCH_BY_WALLET:
                if (strcmp(record->signing_wallet, req->param1.wallet) != 0) return 0;
                break;
            case SEARCH_BY_ROW:
                break;
        }
    }

    if (req->type2 != 0) {
        switch (req->type2) {
            case SEARCH_BY_SLOT:
                if (record->slot != req->param2.slot) return 0;
                break;
            case SEARCH_BY_TX_IDX:
                if (record->tx_idx != req->param2.tx_idx) return 0;
                break;
            case SEARCH_BY_DIRECTION:
                if (strcmp(record->direction, req->param2.direction) != 0) return 0;
                break;
            case SEARCH_BY_WALLET:
                if (strcmp(record->signing_wallet, req->param2.wallet) != 0) return 0;
                break;
            case SEARCH_BY_ROW:
                break;
        }
    }

    return 1;
}

long binary_search_offset(uint64_t target_key) {
    FILE* f = fopen("hashtable.bin", "rb");
    if (!f) {
        perror("Error opening hashtable.bin for binary search");
        return -1;
    }

    fseek(f, 0, SEEK_END);
    long file_size = ftell(f);
    long num_entries = file_size / (sizeof(uint64_t) + sizeof(long));
    rewind(f);

    long left = 0, right = num_entries - 1;
    while (left <= right) {
        long mid = (left + right) / 2;
        fseek(f, mid * (sizeof(uint64_t) + sizeof(long)), SEEK_SET);

        uint64_t key;
        long offset;
        if (fread(&key, sizeof(uint64_t), 1, f) != 1 ||
            fread(&offset, sizeof(long), 1, f) != 1) {
            perror("Error reading hashtable.bin entry");
            fclose(f);
            return -1;
        }

        if (key == target_key) {
            fclose(f);
            return offset;
        } else if (key < target_key) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }

    fclose(f);
    return -1;
}

void combined_search(SearchRequest *req, Record **results, int *count) {
    *count = 0;
    *results = NULL;

    Record record;

    if (req->type1 == SEARCH_BY_ROW) {
        if (req->param1.row < 1 || req->param1.row > meta.record_count) return;
        long offset = (req->param1.row - 1) * sizeof(Record);
        fseek(data_file, offset, SEEK_SET);
        fread(&record, sizeof(Record), 1, data_file);
        *results = malloc(sizeof(Record));
        if (*results) {
            (*results)[0] = record;
            *count = 1;
        }
        return;
    }

    if (req->type2 == SEARCH_BY_ROW) {
        if (req->param2.row < 1 || req->param2.row > meta.record_count) return;
        long offset = (req->param2.row - 1) * sizeof(Record);
        fseek(data_file, offset, SEEK_SET);
        fread(&record, sizeof(Record), 1, data_file);
        *results = malloc(sizeof(Record));
        if (*results) {
            (*results)[0] = record;
            *count = 1;
        }
        return;
    }

    // Binary search lookup for (slot + tx_idx)
    if (req->type1 == SEARCH_BY_SLOT && req->type2 == SEARCH_BY_TX_IDX) {
        uint64_t key = ((uint64_t)req->param1.slot << 32) | req->param2.tx_idx;
        long offset = binary_search_offset(key);
        if (offset >= 0) {
            fseek(data_file, offset, SEEK_SET);
            fread(&record, sizeof(Record), 1, data_file);
            *results = malloc(sizeof(Record));
            if (*results) {
                (*results)[0] = record;
                *count = 1;
            }
        }
        return;
    }

    // Otherwise, scan blocks with criteria filtering
    const size_t block_size = 1000;
    for (unsigned int i = 0; i < meta.block_count; i++) {
        long block_offset = block_index[i].offset;

        size_t records_in_block = (i == meta.block_count - 1)
            ? (meta.record_count % block_size) : block_size;
        if (records_in_block == 0) records_in_block = block_size;

        for (size_t j = 0; j < records_in_block; j++) {
            long offset = block_offset + j * sizeof(Record);
            fseek(data_file, offset, SEEK_SET);
            fread(&record, sizeof(Record), 1, data_file);
            if (matches_criteria(&record, req)) {
                if (*count % 100 == 0) {
                    *results = realloc(*results, (*count + 100) * sizeof(Record));
                    if (!*results) {
                        perror("Memory realloc failed");
                        return;
                    }
                }
                (*results)[*count] = record;
                (*count)++;
            }
        }
    }

    if (*count > 0) {
        *results = realloc(*results, *count * sizeof(Record));
    }
}

int main() {
    signal(SIGINT, cleanup);
    signal(SIGTERM, cleanup);
    signal(SIGSEGV, cleanup);

    int meta_fd = open(METADATA_FILE, O_RDONLY);
    if (meta_fd < 0) {
        perror("Error opening metadata");
        return 1;
    }
    if (read(meta_fd, &meta, sizeof(Metadata)) != sizeof(Metadata)) {
        perror("Error reading metadata");
        close(meta_fd);
        return 1;
    }
    close(meta_fd);

    data_file = fopen(DATA_FILE, "rb");
    if (!data_file) {
        perror("Error opening data file");
        cleanup(0);
        return 1;
    }

    slot_file = fopen(SLOT_INDEX_FILE, "rb");
    if (!slot_file) {
        perror("Error opening slot index file");
        cleanup(0);
        return 1;
    }

    block_index = malloc(meta.block_count * sizeof(BlockIndex));
    if (!block_index) {
        perror("Error allocating block index memory");
        cleanup(0);
        return 1;
    }
    if (fread(block_index, sizeof(BlockIndex), meta.block_count, slot_file) != meta.block_count) {
        perror("Error reading block index");
        cleanup(0);
        return 1;
    }

    mkfifo(REQUEST_PIPE, 0666);

    printf("Server running (PID: %d)\n", getpid());
    printf("Records: %u, Blocks: %u\n", meta.record_count, meta.block_count);

    while (1) {
        printf("Waiting for client requests...\n");
        int request_fd = open(REQUEST_PIPE, O_RDONLY);
        if (request_fd < 0) {
            perror("Error opening request pipe");
            continue;
        }

        SearchRequest req;
        if (read(request_fd, &req, sizeof(SearchRequest)) != sizeof(SearchRequest)) {
            perror("Error reading request");
            close(request_fd);
            continue;
        }
        close(request_fd);

        Record *results = NULL;
        int count = 0;

        combined_search(&req, &results, &count);

        char response_pipe[256];
        snprintf(response_pipe, sizeof(response_pipe), RESPONSE_PIPE_TEMPLATE, req.client_pid);
        int response_fd = open(response_pipe, O_WRONLY);
        if (response_fd < 0) {
            perror("Error opening response pipe");
            if (results) free(results);
            continue;
        }

        if (write(response_fd, &count, sizeof(int)) != sizeof(int)) {
            perror("Error writing result count");
        } else if (count > 0) {
            if (write(response_fd, results, count * sizeof(Record)) != (ssize_t)(count * sizeof(Record))) {
                perror("Error writing records");
            }
            free(results);
        }

        close(response_fd);
        printf("Search complete. Results: %d\n", count);
    }

    return 0;
}

