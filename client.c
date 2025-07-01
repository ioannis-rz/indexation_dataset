#include "common.h"

void display_menu() {
    printf("\nSistema de Busqueda\n");
    printf("1. Seleccionar primer criterio\n");
    printf("2. Seleccionar segundo criterio\n");
    printf("3. Realizar búsqueda\n");
    printf("4. Salir\n");
    printf("Seleccione una opción: ");
}

void display_criteria_menu() {
    printf("\nCriterios de Búsqueda:\n");
    printf("1. Slot\n");
    printf("2. Tx_idx\n");
    printf("3. Dirección (buy/sell)\n");
    printf("4. Wallet\n");
    printf("5. Fila\n");
    printf("Seleccione un criterio: ");
}

void display_record(Record *rec) {
    printf("\n----------------------------------------");
    printf("\nBlock Time: %s", rec->block_time);
    printf("\nSlot: %u", rec->slot);
    printf("\nTx Index: %u", rec->tx_idx);
    printf("\nWallet: %s", rec->signing_wallet);
    printf("\nDirection: %s", rec->direction);
    printf("\nBase Coin: %s", rec->base_coin);
    printf("\nBase Amount: %llu", rec->base_coin_amount);
    printf("\nQuote Amount: %llu", rec->quote_coin_amount);
    printf("\nVirtual token balance: %llu", rec->virtual_token_balance_after);
    printf("\nVirtual sol balance: %llu", rec->virtual_sol_balance_after);
    printf("\nSignature: %s", rec->signature);
    printf("\nProvided gas fee: %llu", rec->provided_gas_fee);
    printf("\nProvided gas limit: %llu", rec->provided_gas_limit);
    printf("\nFee: %llu", rec->fee);
    printf("\nConsumed gas: %llu", rec->consumed_gas);
    printf("\n----------------------------------------\n");
}

int get_criteria_value(SearchType type, void *value, int dato) {
    switch (type) {
        case SEARCH_BY_SLOT:
            printf("Ingrese slot: ");
            return scanf("%u", (unsigned int*)value) == 1;
            
        case SEARCH_BY_TX_IDX:
            printf("Ingrese tx_idx: ");
            return scanf("%u", (unsigned int*)value) == 1;
            
        case SEARCH_BY_DIRECTION:
            printf("Ingrese direccion (buy/sell): ");
            return scanf("%4s", (char*)value) == 1;
            
        case SEARCH_BY_WALLET:
            printf("Ingrese wallet: ");
            return scanf("%49s", (char*)value) == 1;
            
        case SEARCH_BY_ROW:
            printf("Ingrese numero de fila (1 - %u): ", dato);
            return scanf("%u", (unsigned int*)value) == 1;
                        
        default:
            return 0;
    }
}

int main() {
    // Cargar metadatos
    Metadata meta;
    int meta_fd = open(METADATA_FILE, O_RDONLY);
    if (meta_fd < 0) {
        perror("Error abriendo metadatos");
        return 1;
    }
    if (read(meta_fd, &meta, sizeof(Metadata)) != sizeof(Metadata)) {
        perror("Error leyendo metadatos");
        close(meta_fd);
        return 1;
    }
    close(meta_fd);

    int client_pid = getpid();
    char response_pipe[256];
    sprintf(response_pipe, RESPONSE_PIPE_TEMPLATE, client_pid);
    
    if (mkfifo(response_pipe, 0666) < 0 && errno != EEXIST) {
        perror("Error creando tubería de respuesta");
        return 1;
    }

    int option;
    SearchRequest req;
    memset(&req, 0, sizeof(SearchRequest));
    req.client_pid = client_pid;
    req.type1 = 0;
    req.type2 = 0;

    do {
        display_menu();
        scanf("%d", &option);
        getchar();  // Limpiar buffer

        switch(option) {
            case 1:  // Seleccionar primer criterio
                display_criteria_menu();
                scanf("%d", (int*)&req.type1);
                getchar();
                
                if (!get_criteria_value(req.type1, &req.param1, meta.record_count)) {
                    printf("Error: Valor invalido\n");
                    req.type1 = 0;
                }
                break;
                
            case 2:  // Seleccionar segundo criterio
                display_criteria_menu();
                scanf("%d", (int*)&req.type2);
                getchar();
                
                if (!get_criteria_value(req.type2, &req.param2, meta.record_count)) {
                    printf("Error: Valor invalido\n");
                    req.type2 = 0;
                }
                break;
                
            case 3:  // Realizar búsqueda
                if (req.type1 == 0 && req.type2 == 0) {
                    printf("Error: Seleccione al menos un criterio\n");
                    break;
                }
                
                // Enviar solicitud
                int request_fd = open(REQUEST_PIPE, O_WRONLY);
                if (request_fd < 0) {
                    perror("Error abriendo tubería de solicitud");
                    break;
                }
                write(request_fd, &req, sizeof(SearchRequest));
                close(request_fd);

                // Recibir respuesta
                int response_fd = open(response_pipe, O_RDONLY);
                if (response_fd < 0) {
                    perror("Error abriendo tuberia de respuesta");
                    break;
                }

                int count;
                read(response_fd, &count, sizeof(int));

                if (count == 0) {
                    printf("\nNA - No se encontraron resultados\n");
                } else {
                    Record *results = malloc(count * sizeof(Record));
                    read(response_fd, results, count * sizeof(Record));

                    printf("\nResultados encontrados: %d\n", count);
                    for (int i = 0; i < count && i < 10; i++) {
                        printf("\nResultado %d:", i + 1);
                        display_record(&results[i]);
                    }
                    
                    if (count > 10) {
                        printf("\nMostrando 10 de %d resultados\n", count);
                    }
                    free(results);
                }
                close(response_fd);
                break;
                
            case 4:
                printf("Saliendo...\n");
                break;
                
            default:
                printf("Opción invalida\n");
        }
    } while (option != 4);

    unlink(response_pipe);
    return 0;
}