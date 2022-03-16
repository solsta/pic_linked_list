#include <stdio.h>
#include <sys/mman.h>
#include <time.h>
#include <stdbool.h>
#include <string.h>
#include <libpmemobj/base.h>
#include <libpmemobj/pool_base.h>
#include <libpmemobj/types.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#define MESSAGE_BUFFER_SIZE 300

struct node{
    char payload;
    long next;
};

struct my_root {
    bool tx_is_running;
    int log_ops;
    int processed_log_entries;
    char log_file_path[200];
    char log_status;
    int recovery_count;
    long offset;
    long malloc_offset;
    bool instrument_malloc;
    char pmem_start[128000000];
};

struct linked_list{
    bool initialized;
    long size;
    int first;
    struct node index_node[10000000];
};

void sm_op_begin(){}
void sm_op_end(){}

void insert_node(struct linked_list *ll, char payload){
    sm_op_begin();
    if(ll->size == 0){
        ll->first = 0;
        struct node *new_node = &ll->index_node[0];
        new_node->payload = payload;
        ll->size = 1;
    } else if(ll->size == 1){
        struct node *new_node = &ll->index_node[1];
        new_node->payload = payload;
        struct node *fn = &ll->index_node[0];
        fn->next = 1;
        ll->size = 2;
    } else{
        struct node *new_node = &ll->index_node[ll->size];
        new_node->payload = payload;
        struct node *current_last = &ll->index_node[ll->size-1];
        current_last->next = ll->size;
        ll->size = ll->size+1;
    }
    sm_op_end();
}

void remove_node(struct linked_list *ll){
    sm_op_begin();
    struct node *current_node = &ll->index_node[ll->first];
    if(ll->size == 0){
        printf("List is empty!\n");
    } else if(ll->size == 1){
        printf("Last node! Node payload: %c\n", current_node->payload);
        current_node = NULL;
        ll->first = 0;
        ll->size = 0;
    } else{
        long current_index = 0;
        while(current_index < ll->size-1){
            current_index++;
            current_node = &ll->index_node[current_node->next];
        }
        printf("Removing node at index: %lu payload: %c\n",current_index, current_node->payload);
        current_node->next = -1;
        ll->size = ll->size-1;
    }
    sm_op_end();
}

void print_list(struct linked_list *ll){
    printf("List size: %lu\n", ll->size);
    struct node *tmp = &ll->index_node[ll->first];
    int current_index = 0;
    if(tmp == NULL){
        printf("List is empty\n");
    } else{
        while(current_index<ll->size){
            printf("Node: %c\n", tmp->payload);
            current_index++;
            tmp = &ll->index_node[tmp->next];
        }
    }
}

void print_list_silent(struct linked_list *ll){
    printf("List size: %lu\n", ll->size);
    struct node *tmp = &ll->index_node[ll->first];
    int current_index = 0;
    if(tmp == NULL){
        printf("List is empty\n");
    } else{
        while(current_index<ll->size){
            current_index++;
            tmp = &ll->index_node[tmp->next];
        }
    }
}

void run_graceful_exit(){}


void execute_command(char *command, struct linked_list *ll){

    if(strcmp(command, "t") == 0){
        clock_t begin = clock();
        long number_of_opertions = 100000;
        for(long i = 0; i < number_of_opertions; i++){
            insert_node(ll, 'a');
            if(i % (number_of_opertions/100) == 0){
                //printf("Processed: %ld MB\n", i/1000000);
            }
        }

        clock_t end = clock();
        double time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
        printf("Took: %f seconds\n", time_spent);

        clock_t begin_read = clock();
        print_list_silent(ll);
        clock_t end_read = clock();
        double time_spent_read = (double)(end_read - begin_read) / CLOCKS_PER_SEC;
        printf("Took: %f seconds\n", time_spent_read);

    }

    if(strcmp(command, "i") == 0){
        insert_node(ll, 'a');
    }
    if(strcmp(command, "d") == 0){
        remove_node(ll);
    }
    if(strcmp(command, "pri") == 0){
        print_list(ll);

    }
    if(strcmp(command, "e") == 0){
        run_graceful_exit();
    }
}

void execute_recovery_procedure(struct linked_list *ll){
    PMEMobjpool *pool_obj_ptr;
    if ((pool_obj_ptr = pmemobj_open("/mnt/dax/test_outputs/mmaped_files/unique_pmem_file", POBJ_LAYOUT_NAME(list))) == NULL) {
        perror("failed to open pool\n");
    }
    PMEMoid root = pmemobj_root(pool_obj_ptr, sizeof(struct my_root));
    struct my_root *rootp = pmemobj_direct(root);
    memcpy(ll,rootp->pmem_start, sizeof (struct linked_list));
    pmemobj_close(pool_obj_ptr);
}



void execute_command_decorator(char *command, struct linked_list *ll, int connection_file_descriptor){
    char response[10];
    if(strcmp(command, "i") == 0 || strcmp(command, "d") == 0 || strcmp(command, "e") == 0 || strcmp(command, "t") == 0){
        clock_t begin = clock();
        write(connection_file_descriptor, command, strlen(command));
        bzero(response, strlen(response));
        read(connection_file_descriptor, response, strlen(response));
        clock_t end = clock();
        double time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
        printf("Took: %f seconds\n", time_spent);
    }
    printf("Passing to execution function\n");
    execute_command(command,ll);
}

void event_loop(int connection_file_descriptor, struct linked_list *ll){
    printf("Type in action:\n");
    char command[10];
    fgets(command,10,stdin);
    if(strcmp(command, "bulk_send") == 0){
        //Send/Receive 10000 ops
        clock_t begin = clock();
        for(int i = 0; i < 100000; i ++){
            char msg[10];
            bzero(msg, strlen(msg));
            msg[0] = 'i';
            command[strcspn(command, "\r\n")] = 0;
            write(connection_file_descriptor, msg, strlen(msg));
            bzero(msg, strlen(msg));
            read(connection_file_descriptor, msg, strlen(command));
        }
        clock_t end = clock();
        double time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
        printf("Took: %f seconds\n", time_spent);
    } else{
        command[strcspn(command, "\r\n")] = 0;
        execute_command_decorator(command,ll, connection_file_descriptor);
    }

    event_loop(connection_file_descriptor, ll);
}

void start_state_machine_front_end_with_log_server(int server_port_number, struct linked_list *ll){
    int socket_desc;
    struct sockaddr_in server;
    socket_desc = socket(AF_INET, SOCK_STREAM, 0);
    if(socket_desc == -1){
        printf("Could not create socket\n");
        exit(-1);
    }
    server.sin_addr.s_addr = inet_addr("127.0.0.1");
    server.sin_family = AF_INET;
    server.sin_port = htons(server_port_number);

    if(connect(socket_desc, (struct sockaddr *)&server, sizeof(server)) < 0){
        puts("Connection error");
        exit(1);
    }
    event_loop(socket_desc, ll);
}

void server_event_loop(int connection_file_descriptor, struct linked_list *ll){
    char msg_buffer[MESSAGE_BUFFER_SIZE];

    for(;;){
        bzero(msg_buffer, MESSAGE_BUFFER_SIZE);
        read(connection_file_descriptor, msg_buffer, MESSAGE_BUFFER_SIZE);

        execute_command(msg_buffer, ll);

        printf("From client: %s\n", msg_buffer);
        if(strcmp("e", msg_buffer) == 0){
            printf("Received exit command\n");
            close(connection_file_descriptor);
            break;
        }
        bzero(msg_buffer, MESSAGE_BUFFER_SIZE);
        char *server_response_message = "1\n";
        write(connection_file_descriptor, server_response_message, strlen(server_response_message));

    }
    printf("Post loop\n");
    close(connection_file_descriptor);
}

void start_state_machine_backend_end_with_log_server(int psmr_port_number, struct linked_list *ll){

    int socket_desc, connection_file_descriptor, c;
    socket_desc = socket(AF_INET, SOCK_STREAM, 0);
    printf("Created socket\n");
    if(socket_desc == -1){
        printf("Could not create socket\n");
        exit(-1);
    }

    struct sockaddr_in server, client;
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = INADDR_ANY;
    server.sin_port = htons(psmr_port_number);

    if(bind(socket_desc, (struct sockaddr *)&server, sizeof (server)) < 0){
        perror("Bind failed");
        exit(1);
    }
    puts("Bind is done");
    listen(socket_desc, 3);

    puts("Waiting for incoming connections..");
    c = sizeof (struct sockaddr_in);

    connection_file_descriptor = accept(socket_desc, (struct sockaddr *)&client, (socklen_t*)&c);
    if (connection_file_descriptor < 0) {
        perror("Accept failed");
        exit(1);
    }
    printf("Starting a chat\n");
    server_event_loop(connection_file_descriptor, ll);
    close(socket_desc);
}

void interactve_state_machine_loop(struct linked_list *ll){
    /* Wait for the command here */
    printf("Type in action:\n");
    char command[10];
    fgets(command,10,stdin);
    command[strcspn(command, "\r\n")] = 0;
    execute_command(command, ll);
    interactve_state_machine_loop(ll);
}

void start_state_machine(struct linked_list *ll){
    interactve_state_machine_loop(ll);
}

int main(int argc, const char *argv[]) {
    struct linked_list *ll = mmap(NULL, sizeof (struct linked_list),PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, 0, 0);
    if(strcmp(argv[1], "producer") == 0){
        printf("Starting producer\n");


        printf("Recover from psm?");
        char command[10];
        fgets(command,10,stdin);
        command[strcspn(command, "\r\n")] = 0;
        if(strcmp(command, "y") == 0){
            execute_recovery_procedure(ll);
            printf("Recovery is complete!\n");
        } else{
            printf("Number of elements: %lu\n", ll->size);
            if(!ll->initialized){
                ll->size = 0;
                ll->initialized = true;
            }
        }

        printf("Enter the port to connect to:\n?");
        bzero(command, 10);
        fgets(command,10,stdin);
        command[strcspn(command, "\r\n")] = 0;
        int server_port_number = atoi(command);
        printf("Selected port: %d\n", server_port_number);

        start_state_machine_front_end_with_log_server(server_port_number, ll);
    } else if(strcmp(argv[1], "consumer") == 0){
        printf("Starting consumer\n");
        int server_port_number = atoi(argv[2]);
        start_state_machine_backend_end_with_log_server(server_port_number, ll);
    } else if(strcmp(argv[1], "standalone_pmem") == 0){
        start_state_machine(ll);
    }

    run_graceful_exit();
    return 0;
}
