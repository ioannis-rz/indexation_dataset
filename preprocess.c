#include "common.h"
#include <stdlib.h>
#include <stdio.h>

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("Usage: %s <input_csv>\n", argv[0]);
        return 1;
    }

    FILE *csv = fopen(argv[1], "r");
    if (!csv) {
        perror("Error opening CSV file");
        return 1;
    }

    // Crear archivos binarios
    FILE *data_file = fopen(DATA_FILE, "wb");
    FILE *hash_file = fopen(HASH_INDEX_FILE, "wb");
    FILE *slot_file = fopen(SLOT_INDEX_FILE, "wb");
    FILE *meta_file = fopen(METADATA_FILE, "wb");
    
    if (!data_file || !hash_file || !slot_file || !meta_file) {
        perror("Error opening output files");
        fclose(csv);
        if (data_file) fclose(data_file);
        if (hash_file) fclose(hash_file);
        if (slot_file) fclose(slot_file);
        if (meta_file) fclose(meta_file);
        return 1;
    }

    char line[1024];
    // Saltar encabezado
    fgets(line, sizeof(line), csv);
    
    Record record;
    HashEntry hentry;
    BlockIndex block_idx = {0};
    Metadata meta = {
        .record_count = 0,
        .block_count = 0,
        .record_size = sizeof(Record)
    };
    
    const int BLOCK_SIZE = 1000; // Registros por bloque
    long current_offset = 0;

    while (fgets(line, sizeof(line), csv)) {
        // Parsear línea CSV
        sscanf(line, "%19[^,],%u,%u,%49[^,],%4[^,],%99[^,],%llu,%llu,%llu,%llu,%99[^,],%lu,%lu,%lu,%lu",
               record.block_time, &record.slot, &record.tx_idx, record.signing_wallet, 
               record.direction, record.base_coin, &record.base_coin_amount, 
               &record.quote_coin_amount, &record.virtual_token_balance_after, 
               &record.virtual_sol_balance_after, record.signature, 
               &record.provided_gas_fee, &record.provided_gas_limit, 
               &record.fee, &record.consumed_gas);

        // Escribir registro en data.bin
        fwrite(&record, sizeof(Record), 1, data_file);
        
        // Actualizar índice hash
        hentry.slot = record.slot;
        hentry.tx_idx = record.tx_idx;
        hentry.offset = current_offset;
        fwrite(&hentry, sizeof(HashEntry), 1, hash_file);
        
        // Actualizar índice de bloques
        if (meta.record_count % BLOCK_SIZE == 0) {
            if (meta.block_count > 0) {
                // Guardar bloque anterior
                fwrite(&block_idx, sizeof(BlockIndex), 1, slot_file);
            }
            block_idx.min_slot = record.slot;
            block_idx.max_slot = record.slot;
            block_idx.offset = current_offset;
            meta.block_count++;
        } else {
            if (record.slot < block_idx.min_slot) block_idx.min_slot = record.slot;
            if (record.slot > block_idx.max_slot) block_idx.max_slot = record.slot;
        }
        
        current_offset += sizeof(Record);
        meta.record_count++;
    }
    
    // Guardar último bloque
    fwrite(&block_idx, sizeof(BlockIndex), 1, slot_file);
    
    // Guardar metadatos
    fwrite(&meta, sizeof(Metadata), 1, meta_file);
    
    // Limpiar
    fclose(csv);
    fclose(data_file);
    fclose(hash_file);
    fclose(slot_file);
    fclose(meta_file);
    
    printf("Preprocessing completed. Records: %d, Blocks: %d\n", 
           meta.record_count, meta.block_count);
    return 0;
}