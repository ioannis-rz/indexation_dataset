#include "common.h"

// Variables globales
Metadata meta;
BlockIndex *block_index = NULL;
int data_fd = -1;
int slot_fd = -1;

void cleanup(int sig) {
    printf("\nSeñal %d recibida. Limpiando recursos...\n", sig);
    
    if (block_index) {
        munmap(block_index, meta.block_count * sizeof(BlockIndex));
        block_index = NULL;
    }
    if (data_fd >= 0) close(data_fd);
    if (slot_fd >= 0) close(slot_fd);
    unlink(REQUEST_PIPE);
    exit(0);
}

int matches_criteria(Record *record, SearchRequest *req) {
    // Verificar primer criterio
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
                // No aplica aquí, se maneja en búsqueda directa
                break;
        }
    }
    
    // Verificar segundo criterio
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
                // No aplica aquí
                break;
        }
    }
    
    return 1;
}

void combined_search(SearchRequest *req, Record **results, int *count) {
    *count = 0;
    *results = NULL;
    
    // Caso especial: búsqueda por fila
    if (req->type1 == SEARCH_BY_ROW) {
        if (req->param1.row < 1 || req->param1.row > meta.record_count) return;
        
        long offset = (req->param1.row - 1) * sizeof(Record);
        lseek(data_fd, offset, SEEK_SET);
        
        Record *record = malloc(sizeof(Record));
        if (read(data_fd, record, sizeof(Record)) == sizeof(Record)) {
            *results = record;
            *count = 1;
        } else {
            free(record);
        }
        return;
    }
    
    if (req->type2 == SEARCH_BY_ROW) {
        if (req->param2.row < 1 || req->param2.row > meta.record_count) return;
        
        long offset = (req->param2.row - 1) * sizeof(Record);
        lseek(data_fd, offset, SEEK_SET);
        
        Record *record = malloc(sizeof(Record));
        if (read(data_fd, record, sizeof(Record)) == sizeof(Record)) {
            *results = record;
            *count = 1;
        } else {
            free(record);
        }
        return;
    }
    
    // Búsqueda combinada en bloques
    for (unsigned int i = 0; i < meta.block_count; i++) {
        lseek(data_fd, block_index[i].offset, SEEK_SET);
        int block_size = (i == meta.block_count - 1) ? 
            (meta.record_count % 1000) : 1000;
        if (block_size == 0) block_size = 1000;
        
        Record *block = malloc(block_size * sizeof(Record));
        read(data_fd, block, block_size * sizeof(Record));
        
        for (int j = 0; j < block_size; j++) {
            if (matches_criteria(&block[j], req)) {
                *results = realloc(*results, (*count + 1) * sizeof(Record));
                (*results)[*count] = block[j];
                (*count)++;
            }
        }
        free(block);
    }
}

int main() {
    signal(SIGINT, cleanup);
    signal(SIGTERM, cleanup);
    signal(SIGSEGV, cleanup);

    // Cargar metadatos
    int meta_fd = open(METADATA_FILE, O_RDONLY);
    if (meta_fd < 0) {
        perror("Error abriendo metadatos");
        return 1;
    }
    read(meta_fd, &meta, sizeof(Metadata));
    close(meta_fd);

    // Abrir archivos de datos
    data_fd = open(DATA_FILE, O_RDONLY);
    slot_fd = open(SLOT_INDEX_FILE, O_RDONLY);
    
    // Mapear índice
    block_index = mmap(NULL, meta.block_count * sizeof(BlockIndex), 
                      PROT_READ, MAP_SHARED, slot_fd, 0);

    // Crear tubería de solicitud
    mkfifo(REQUEST_PIPE, 0666);

    printf("Servidor combinado iniciado (PID: %d)\n", getpid());
    printf("Registros: %u, Bloques: %u, Memoria: %.2f MB\n", 
           meta.record_count, meta.block_count,
           (meta.block_count * sizeof(BlockIndex)) / (1024.0 * 1024.0));

    while (1) {
        int request_fd = open(REQUEST_PIPE, O_RDONLY);
        SearchRequest req;
        read(request_fd, &req, sizeof(SearchRequest));
        close(request_fd);

        Record *results = NULL;
        int count = 0;
        
        combined_search(&req, &results, &count);

        char response_pipe[256];
        sprintf(response_pipe, RESPONSE_PIPE_TEMPLATE, req.client_pid);
        int response_fd = open(response_pipe, O_WRONLY);
        
        write(response_fd, &count, sizeof(int));
        if (count > 0) {
            write(response_fd, results, count * sizeof(Record));
            free(results);
        }
        close(response_fd);
    }

    return 0;
}