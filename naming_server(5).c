// naming_server.c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <stdbool.h>
#include <asm-generic/socket.h>
#include <stdbool.h>
#include <stdarg.h>
#include <time.h>
#include <sys/time.h>
#include <signal.h>
#include <stdarg.h>

#define NM_PORT 8080
#define BUFFER_SIZE 1024
#define MAX_PATH_LENGTH 256
#define LRU_CACHE_SIZE 100
#define LOG_FILE "naming_server.log"

#define MAX_PATHS 200
typedef struct
{
    char ip[16];
    int port;
    int transfer_port;
    char **accessible_paths;
    int num_paths;
    int socket;
} storage_server_info;

typedef struct
{
    int sock;
    char buffer[BUFFER_SIZE];
} ThreadArgs;

typedef struct TrieNode
{
    struct TrieNode *children[400];
    int server_index;
    int is_end;
} TrieNode;

typedef struct LRUNode
{
    char *path;
    int server_index;
    struct LRUNode *prev;
    struct LRUNode *next;
} LRUNode;

typedef struct
{
    int capacity;
    int size;
    LRUNode *head;
    LRUNode *tail;
    LRUNode **hash_table;
} LRUCache;

#define MAX_STORAGE_SERVERS 10
storage_server_info storage_servers[MAX_STORAGE_SERVERS];
int num_storage_servers = 0;
pthread_mutex_t servers_mutex = PTHREAD_MUTEX_INITIALIZER;

TrieNode *root = NULL;
LRUCache *cache = NULL;

TrieNode *create_trie_node();
LRUCache *create_lru_cache(int capacity);
void insert_path(const char *path, int server_index);
void put_in_cache(LRUCache *cache, const char *path, int server_index);
int search_path(const char *path);
int get_from_cache(LRUCache *cache, const char *path);

void free_trie(TrieNode *root);
void free_storage_servers();

void *handle_storage_server(int *client_sock_ptr);
void *handle_client(void *args);
int checkifclient(char *buffer);

char *get_name_from_path(const char *path);
void get_parent_path(const char *full_path, char *parent_path, size_t size);

int send_create_command(const char *type, const char *name, const char *path);
int send_delete_command(const char *type, const char *name, const char *path);
int send_copy_command(const char *type, const char *src_path, const char *dest_path);

void initialize_naming_server();
void handle_shutdown(int signum);

void log_event(const char *format, ...);
void log_separator(const char *message);

TrieNode *create_trie_node()
{
    TrieNode *node = (TrieNode *)calloc(1, sizeof(TrieNode));
    node->server_index = -1;
    return node;
}

void insert_path(const char *path, int server_index)
{
    if (!root)
    {
        root = create_trie_node();
    }

    TrieNode *current = root;
    printf("%s\n", path);
    while (*path)
    {
        unsigned char index = (unsigned char)*path;
        if (!current->children[index])
        {
            printf("%c ", index);
            current->children[index] = create_trie_node();
        }
        current = current->children[index];
        path++;
    }
    current->is_end = 1;
    current->server_index = server_index;
    printf("%d\n", server_index);
}

int search_path(const char *path)
{
    int cached_result = get_from_cache(cache, path);
    if (cached_result != -1)
    {
        return cached_result;
    }

    if (!root || !path)
        return -1;

    const char *original_path = path;
    size_t path_len = strlen(path);

    while (path_len > 0)
    {
        TrieNode *current = root;
        const char *curr_pos = original_path;

        size_t pos = 0;
        while (pos < path_len)
        {
            unsigned char index = (unsigned char)*curr_pos;
            if (current->children[index] == NULL)
            {
                break;
            }
            current = current->children[index];
            curr_pos++;
            pos++;
        }

        if (pos == path_len && current->is_end)
        {
            put_in_cache(cache, original_path, current->server_index);
            return current->server_index;
        }

        while (path_len > 0 && original_path[path_len - 1] != '/')
        {
            path_len--;
        }
        if (path_len > 0)
        {
            path_len--;
        }
    }

    // for (int i = 0; i < num_storage_servers; i++)
    // {
    //     for (int j = 0; j < storage_servers[i].num_paths; j++)
    //     {
    //         printf("%s 1 %s 1\n",path,storage_servers[i].accessible_paths[j]);
    //         if(strcmp(path, storage_servers[i].accessible_paths[j]) == 0)
    //         {
    //             return i;
    //         }
    //     }
    // }

    return -1;
}

LRUCache *create_lru_cache(int capacity)
{
    LRUCache *cache = (LRUCache *)malloc(sizeof(LRUCache));
    cache->capacity = capacity;
    cache->size = 0;
    cache->head = NULL;
    cache->tail = NULL;
    cache->hash_table = (LRUNode **)calloc(capacity, sizeof(LRUNode *));
    return cache;
}

int get_from_cache(LRUCache *cache, const char *path)
{
    unsigned long hash = 5381;
    const char *str = path;
    while (*str)
    {
        hash = ((hash << 5) + hash) + *str++;
    }
    int index = hash % cache->capacity;

    LRUNode *node = cache->hash_table[index];
    if (node && strcmp(node->path, path) == 0)
    {
        if (node != cache->tail)
        {
            if (node == cache->head)
            {
                cache->head = node->next;
                if (cache->head)
                {
                    cache->head->prev = NULL;
                }
            }
            else
            {
                node->prev->next = node->next;
                if (node->next)
                {
                    node->next->prev = node->prev;
                }
            }

            node->prev = cache->tail;
            node->next = NULL;
            if (cache->tail)
            {
                cache->tail->next = node;
            }
            cache->tail = node;
        }

        return node->server_index;
    }
    return -1;
}

void put_in_cache(LRUCache *cache, const char *path, int server_index)
{
    unsigned long hash = 5381;
    const char *str = path;
    while (*str)
    {
        hash = ((hash << 5) + hash) + *str++;
    }
    int index = hash % cache->capacity;

    LRUNode *new_node = (LRUNode *)malloc(sizeof(LRUNode));
    new_node->path = strdup(path);
    new_node->server_index = server_index;
    new_node->prev = cache->tail;
    new_node->next = NULL;

    cache->hash_table[index] = new_node;

    if (cache->tail)
    {
        cache->tail->next = new_node;
    }
    cache->tail = new_node;

    if (!cache->head)
    {
        cache->head = new_node;
    }

    if (cache->size >= cache->capacity)
    {
        LRUNode *old_head = cache->head;
        cache->head = old_head->next;
        if (cache->head)
        {
            cache->head->prev = NULL;
        }

        unsigned long old_hash = 5381;
        str = old_head->path;
        while (*str)
        {
            old_hash = ((old_hash << 5) + old_hash) + *str++;
        }
        int old_index = old_hash % cache->capacity;
        cache->hash_table[old_index] = NULL;

        free(old_head->path);
        free(old_head);
    }
    else
    {
        cache->size++;
    }
}

void free_trie(TrieNode *root)
{
    if (root == NULL)
        return;

    for (int i = 0; i < 256; i++)
    {
        if (root->children[i])
        {
            free_trie(root->children[i]);
        }
    }
    free(root);
}

void initialize_naming_server()
{
    root = create_trie_node();
    cache = create_lru_cache(LRU_CACHE_SIZE);
}

int checkifclient(char *buffer)
{
    char operation[32];
    char path[256];
    sscanf(buffer, "%s %s", operation, path);
    return (strcmp(operation, "GET_SERVER") == 0 || strcmp(operation, "CREATE") == 0 ||
            strcmp(operation, "DELETE") == 0 || strcmp(operation, "COPY") == 0 ||
            strcmp(operation, "STREAM") == 0);
}

void log_event(const char *format, ...)
{
    va_list args;
    va_start(args, format);

    FILE *log_file = fopen(LOG_FILE, "a");
    if (log_file)
    {
        time_t now = time(NULL);
        struct tm *time_info = localtime(&now);

        char timestamp[20];
        if (time_info && strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", time_info))
        {
            fprintf(log_file, "[%s] ", timestamp);
            vfprintf(log_file, format, args);
            fprintf(log_file, "\n");
        }
        else
        {
            fprintf(log_file, "[UNKNOWN TIME] ");
            vfprintf(log_file, format, args);
            fprintf(log_file, "\n");
        }

        fclose(log_file);
    }
    else
    {
        fprintf(stderr, "ERROR: Unable to open log file '%s' for writing.\n", LOG_FILE);
    }

    va_end(args);
}

void log_separator(const char *message)
{
    FILE *log_file = fopen(LOG_FILE, "a");
    if (log_file)
    {
        fprintf(log_file, "\n");
        fprintf(log_file, "===============================================================\n");
        fprintf(log_file, "%s\n", message);
        fprintf(log_file, "===============================================================\n");
        fprintf(log_file, "\n");

        fclose(log_file);
    }
    else
    {
        fprintf(stderr, "ERROR: Unable to open log file '%s' for writing.\n", LOG_FILE);
    }
}

char *get_name_from_path(const char *path)
{
    char *last_slash = strrchr(path, '/');
    if (last_slash != NULL)
    {
        return last_slash + 1;
    }
    return (char *)path;
}

void get_parent_path(const char *full_path, char *parent_path, size_t size)
{
    strncpy(parent_path, full_path, size);
    parent_path[size - 1] = '\0';
    char *last_slash = strrchr(parent_path, '/');
    if (last_slash != NULL)
    {
        *last_slash = '\0';
    }
}

void handle_shutdown(int signum)
{
    printf("Shutting down Naming Server...\n");
    log_event("Shutting down Naming Server...");
    free_trie(root);
    free_storage_servers();
    exit(EXIT_SUCCESS);
}

void free_storage_servers()
{
    pthread_mutex_lock(&servers_mutex);
    for (int i = 0; i < num_storage_servers; i++)
    {
        for (int j = 0; j < storage_servers[i].num_paths; j++)
        {
            free(storage_servers[i].accessible_paths[j]);
        }
        free(storage_servers[i].accessible_paths);
    }
    pthread_mutex_unlock(&servers_mutex);
}

int main()
{
    initialize_naming_server();

    int server_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock < 0)
    {
        log_event("Failed to create server socket");
        return EXIT_FAILURE;
    }

    if (setsockopt(server_sock, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int)) < 0)
    {
        log_event("Failed to set socket options");
        close(server_sock);
        return EXIT_FAILURE;
    }

    if (setsockopt(server_sock, SOL_SOCKET, SO_REUSEPORT, &(int){1}, sizeof(int)) < 0)
    {
        log_event("Failed to set socket options");
        close(server_sock);
        return EXIT_FAILURE;
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(NM_PORT);
    serv_addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        log_event("Failed to bind server socket");
        close(server_sock);
        return EXIT_FAILURE;
    }

    if (listen(server_sock, 5) < 0)
    {
        log_event("Failed to listen on server socket");
        close(server_sock);
        return EXIT_FAILURE;
    }

    log_separator("Starting new server session: Running Naming server on port 8080");

    while (1)
    {
        struct sockaddr_in cli_addr;
        socklen_t cli_len = sizeof(cli_addr);
        int client_sock = accept(server_sock, (struct sockaddr *)&cli_addr, &cli_len);
        if (client_sock < 0)
        {
            log_event("Failed to accept connection");
            continue;
        }

        char initial_message[BUFFER_SIZE];
        int bytes_received = recv(client_sock, initial_message, sizeof(initial_message) - 1, 0);
        if (bytes_received <= 0)
        {
            log_event("Failed to receive data from connection");
            close(client_sock);
            continue;
        }
        initial_message[bytes_received] = '\0';

        log_event("Received connection: %s", initial_message);

        ThreadArgs *args = malloc(sizeof(ThreadArgs));
        if (args == NULL)
        {
            log_event("Failed to allocate thread arguments");
            close(client_sock);
            continue;
        }
        args->sock = client_sock;
        strncpy(args->buffer, initial_message, BUFFER_SIZE - 1);
        args->buffer[BUFFER_SIZE - 1] = '\0';

        if (strstr(initial_message, "Storage Server") != NULL)
        {
            pthread_t storage_thread;
            if (pthread_create(&storage_thread, NULL, handle_storage_server, args) != 0)
            {
                log_event("Failed to create storage server thread");
                free(args);
                close(client_sock);
            }
            else
            {
                pthread_detach(storage_thread);
            }
        }
        else if (checkifclient(initial_message))
        {
            pthread_t client_thread;
            if (pthread_create(&client_thread, NULL, handle_client, args) != 0)
            {
                log_event("Failed to create client thread");
                free(args);
                close(client_sock);
            }
            else
            {
                pthread_detach(client_thread);
            }
        }
        else
        {
            log_event("Unknown connection type");
            free(args);
            close(client_sock);
        }
    }

    close(server_sock);

    free_trie(root);
    free_storage_servers();
    return EXIT_SUCCESS;
}

//          0: Success
//          1: Path not accessible
//          2: Send error
//          3: Receive error
//          4: System error (includes unknown responses)
//          5: Invalid input format
//          6: Path already exists
//          7: Connection error
//          8: Path limit exceeded
//          9: Path does/does not exist

int send_create_command(const char *type, const char *name, const char *path)
{
    if (!type || !name || !path)
    {
        log_event("Invalid input parameters: type/name/path is NULL");
        return 5;
    }

    char full_path[BUFFER_SIZE];
    strcpy(full_path, path);
    strcat(full_path, "/");
    strcat(full_path, name);

    if (!name)
    {
        log_event("Invalid path format: Unable to extract name");
        return 5;
    }

    int server = search_path(path);
    if (server < 0)
    {
        log_event("Path is not accessible: %s", path);
        return 1;
    }

    if (search_path(full_path) >= 0)
    {
        log_event("Path already exists: %s", full_path);
        return 6;
    }

    int client_sock = storage_servers[server].socket;
    if (client_sock < 0)
    {
        log_event("Invalid socket connection");
        return 7;
    }

    char command[BUFFER_SIZE];
    if (snprintf(command, sizeof(command), "CREATE %s %s", type, full_path) >= sizeof(command))
    {
        log_event("Command buffer overflow");
        return 5;
    }

    if (send(client_sock, command, strlen(command), 0) < 0)
    {
        log_event("Failed to send CREATE command: %s", command);
        return 2;
    }

    log_event("Sent CREATE command to Storage Server: %s", command);

    char response[BUFFER_SIZE] = {0};
    int bytes_received = recv(client_sock, response, sizeof(response) - 1, 0);
    if (bytes_received > 0)
    {
        response[bytes_received] = '\0';
        log_event("Received response from Storage Server: %s", response);

        if (bytes_received < 1)
        {
            return 4;
        }

        int response_code = response[0] - '0';
        switch (response_code)
        {
        case 0:
            if (storage_servers[server].num_paths >= MAX_PATHS)
            {
                log_event("Maximum path limit reached");
                return 0;
            }
            storage_servers[server].accessible_paths[storage_servers[server].num_paths++] = strdup(full_path);
            insert_path(full_path, server);
            return 0;
        case 1:
            return 4;
        case 2:
            return 5;
        case 3:
            return 6;
        default:
            return 4;
        }
    }
    else
    {
        log_event("Failed to receive response from Storage Server");
        return 3;
    }
}

int send_delete_command(const char *type, const char *name, const char *path)
{
    if (!type || !name || !path)
    {
        log_event("Invalid input parameters: type/name/path is NULL");
        return 5;
    }

    char full_path[BUFFER_SIZE];
    strcpy(full_path, path);
    strcat(full_path, "/");
    strcat(full_path, name);

    int server = search_path(path);
    if (server < 0)
    {
        log_event("Path is not accessible: %s", path);
        return 1;
    }

    if (search_path(full_path) < 0)
    {
        log_event("Path does not exist: %s", full_path);
        return 9;
    }

    int client_sock = storage_servers[server].socket;
    if (client_sock < 0)
    {
        log_event("Invalid socket connection");
        return 7;
    }

    char command[BUFFER_SIZE];
    if (snprintf(command, sizeof(command), "DELETE %s %s", type, full_path) >= sizeof(command))
    {
        log_event("Command buffer overflow");
        return 5;
    }

    if (send(client_sock, command, strlen(command), 0) < 0)
    {
        log_event("Failed to send DELETE command: %s", command);
        return 2;
    }

    log_event("Sent DELETE command to Storage Server: %s", command);

    char response[BUFFER_SIZE] = {0};
    int bytes_received = recv(client_sock, response, sizeof(response) - 1, 0);
    if (bytes_received > 0)
    {
        response[bytes_received] = '\0';
        log_event("Received response from Storage Server: %s", response);

        int response_code = response[0] - '0';
        switch (response_code)
        {
        case 0:
            for (int i = 0; i < storage_servers[server].num_paths; i++)
            {
                if (strcmp(storage_servers[server].accessible_paths[i], full_path) == 0)
                {
                    for (int j = i; j < storage_servers[server].num_paths - 1; j++)
                    {
                        storage_servers[server].accessible_paths[j] =
                            storage_servers[server].accessible_paths[j + 1];
                    }
                    storage_servers[server].num_paths--;

                    // if (remove_path(full_path) != 0)
                    // {
                    //     printf("Warning: Failed to remove path from trie\n");
                    //     log_event("Warning: Failed to remove path from trie");
                    // }
                    //
                    //

                    break;
                }
            }
            return 0;
        case 1:
            return 4;
        case 2:
            return 5;
        case 4:
            return 9;
        default:
            return 4;
        }
    }
    else
    {
        log_event("Failed to receive response from Storage Server");
        return 3;
    }
}

int send_copy_command(const char *type, const char *src_path, const char *dest_path)
{
    if (!type || !src_path || !dest_path)
    {
        log_event("Invalid input parameters: type/path is NULL");
        return 5;
    }

    int src_server = search_path(src_path);
    int dest_server = search_path(dest_path);
    if (src_server < 0)
    {
        log_event("Path is not accessible: %s", src_path);
        return 1;
    }

    if (dest_server < 0)
    {
        log_event("Path does not exist: %s", dest_path);
        return 9;
    }

    int dest_sock = storage_servers[dest_server].socket;
    char src_ip[12];
    strcpy(src_ip, storage_servers[src_server].ip);

    if (dest_sock < 0)
    {
        log_event("Invalid socket connection");
        return 7;
    }

    char command[BUFFER_SIZE];
    if (snprintf(command, sizeof(command), "COPY %s %s %s %s\n", type, src_path, dest_path, src_ip) >= sizeof(command))
    {
        log_event("Command buffer overflow");
        return 5;
    }

    if (send(dest_sock, command, strlen(command), 0) < 0)
    {
        log_event("Failed to send COPY command: %s", command);
        return 2;
    }

    log_event("Sent COPY command to Storage Server: %s", command);

    char response[BUFFER_SIZE] = {0};
    int bytes_received = recv(dest_sock, response, sizeof(response) - 1, 0);
    if (bytes_received > 0)
    {
        response[bytes_received] = '\0';
        log_event("Received response from Storage Server: %s", response);

        int response_code = response[0] - '0';
        switch (response_code)
        {
        case 0:
            return 0;
        case 1:
            return 4;
        case 2:
            return 5;
        case 4:
            return 9;
        default:
            return 4;
        }
    }
    else
    {
        log_event("Failed to receive response from Storage Server");
        return 3;
    }
}

void *handle_storage_server(int *client_sock_ptr)
{
    if (!client_sock_ptr)
    {
        log_event("ERROR: NULL client socket pointer");
        return NULL;
    }

    int client_sock = *client_sock_ptr;
    free(client_sock_ptr);

    char buffer[BUFFER_SIZE] = {0};

    int bytes_read = recv(client_sock, buffer, sizeof(buffer) - 1, 0);
    if (bytes_read <= 0)
    {
        log_event("Error receiving data from client: %s", strerror(errno));
        close(client_sock);
        return NULL;
    }

    buffer[bytes_read] = '\0';
    log_event("Received registration: %s", buffer);

    int port = 0;
    char *port_start = strstr(buffer, "Client Port: ");
    if (!port_start)
    {
        log_event("ERROR: Client Port not found in message");
        close(client_sock);
        return NULL;
    }

    port_start += 13;
    char *end_ptr;
    port = strtol(port_start, &end_ptr, 10);
    if (port <= 0 || port > 65535)
    {
        log_event("ERROR: Invalid port number: %d", port);
        close(client_sock);
        return NULL;
    }

    char ip[INET_ADDRSTRLEN] = {0};
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);
    if (getpeername(client_sock, (struct sockaddr *)&addr, &addr_len) < 0)
    {
        log_event("Error getting peer name: %s", strerror(errno));
        close(client_sock);
        return NULL;
    }

    if (!inet_ntop(AF_INET, &addr.sin_addr, ip, sizeof(ip)))
    {
        log_event("Error converting IP address: %s", strerror(errno));
        close(client_sock);
        return NULL;
    }

    pthread_mutex_lock(&servers_mutex);

    if (num_storage_servers >= MAX_STORAGE_SERVERS)
    {
        log_event("ERROR: Maximum number of storage servers reached");
        pthread_mutex_unlock(&servers_mutex);
        close(client_sock);
        return NULL;
    }

    char *paths_start = strstr(buffer, "Paths: ");
    if (!paths_start)
    {
        log_event("ERROR: Paths not found in message");
        pthread_mutex_unlock(&servers_mutex);
        close(client_sock);
        return NULL;
    }
    paths_start += 7;

    int server_index = num_storage_servers;

    storage_server_info *server = &storage_servers[num_storage_servers];
    strncpy(server->ip, ip, INET_ADDRSTRLEN - 1);
    server->ip[INET_ADDRSTRLEN - 1] = '\0';
    server->port = port;
    server->transfer_port = port;
    server->socket = client_sock;

    printf("Path start %s\n", paths_start);
    log_event("Path start %s", paths_start);
    char *path = strtok(paths_start, ",");
    storage_servers[num_storage_servers].accessible_paths = (char **)malloc(sizeof(char *) * 400);
    storage_servers[num_storage_servers].num_paths = 0;
    while (path != NULL)
    {
        storage_servers[num_storage_servers].accessible_paths[storage_servers[num_storage_servers].num_paths] = strdup(path);
        insert_path(path, server_index);
        storage_servers[num_storage_servers].num_paths++;
        path = strtok(NULL, ",");
    }
    num_storage_servers++;
    pthread_mutex_unlock(&servers_mutex);

    const char *ack_message = "Registration confirmed";
    if (send(client_sock, ack_message, strlen(ack_message), 0) < 0)
    {
        printf("Error sending acknowledgment: %s\n", strerror(errno));
        log_event("Error sending acknowledgment: %s", strerror(errno));
        close(client_sock);
        return NULL;
    }

    printf("Storage server registered successfully - IP: %s, Port: %d\n", ip, port);
    log_event("Storage server registered successfully - IP: %s, Port: %d", ip, port);

    memset(buffer, 0, sizeof(buffer));
    bytes_read = recv(client_sock, buffer, sizeof(buffer) - 1, 0);
    if (bytes_read > 0)
    {
        buffer[bytes_read] = '\0';
        log_event("Received response from Storage Server: %s", buffer);
    }
    else
    {
        log_event("Error receiving response from Storage Server: %s", strerror(errno));
    }

    return NULL;
}

void *handle_client(void *arg)
{
    ThreadArgs *args = (ThreadArgs *)arg;
    if (!args)
    {
        log_event("ERROR: NULL thread arguments");
        return NULL;
    }

    int client_sock = args->sock;
    char buffer[BUFFER_SIZE];
    strncpy(buffer, args->buffer, BUFFER_SIZE - 1);
    buffer[BUFFER_SIZE - 1] = '\0';

    free(args);

    log_event("Processing client request: %s", buffer);

    char command[32] = {0};
    char path[MAX_PATH_LENGTH] = {0};
    char name[MAX_PATH_LENGTH] = {0};
    char dest_path[MAX_PATH_LENGTH] = {0};

    int parsed_items = sscanf(buffer, "%31s %511s %511s", command, path, name);
    char response[BUFFER_SIZE];

    if (strcmp(command, "GET_SERVER") == 0)
    {
        if (parsed_items != 2)
        {
            log_event("Invalid input format");
            strcpy(response, "ERROR: Invalid command format");
            send(client_sock, response, strlen(response), 0);
            close(client_sock);
            return NULL;
        }

        pthread_mutex_lock(&servers_mutex);

        log_event("Searching for path: %s", path);
        int server_index = search_path(path);
        log_event("Server index for path %s: %d", path, server_index);

        if (server_index >= 0 && server_index < num_storage_servers)
        {
            snprintf(response, sizeof(response), "%s %d %d",
                     storage_servers[server_index].ip,
                     storage_servers[server_index].port,
                     storage_servers[server_index].transfer_port);
            log_event("Sending server info: IP=%s, Port=%d, Transfer Port=%d",
                      storage_servers[server_index].ip,
                      storage_servers[server_index].port,
                      storage_servers[server_index].transfer_port);
        }
        else
        {
            snprintf(response, sizeof(response), "ERROR: No such path is accessible on any storage server");
            log_event("Path not found: %s", path);
        }
        pthread_mutex_unlock(&servers_mutex);

        if (send(client_sock, response, strlen(response), 0) < 0)
        {
            log_event("Failed to send response to client: %s", strerror(errno));
        }
    }
    else
    {
        if (parsed_items != 3)
        {
            log_event("ERROR: Invalid input format");
            strcpy(response, "ERROR: Invalid input format");
            send(client_sock, response, strlen(response), 0);
            close(client_sock);
            return NULL;
        }

        const char *type = (strchr(name, '.') != NULL) ? "FILE" : "DIR";
        const char *typecopy = (strchr(path, '.') != NULL) ? "FILE" : "DIR";
        int success = -1;

        if (strcmp(command, "CREATE") == 0)
        {
            log_event("CREATE command - Type: %s, Path: %s, Name: %s", type, path, name);

            success = send_create_command(type, name, path);
        }
        else if (strcmp(command, "DELETE") == 0)
        {
            log_event("DELETE command - Type: %s, Path: %s, Name: %s", type, path, name);

            success = send_delete_command(type, name, path);
        }
        else if (strcmp(command, "COPY") == 0)
        {
            log_event("COPY command - Type: %s, Path: %s, Destination: %s", typecopy, path, name);
            success = send_copy_command(typecopy, path, name);
        }
        else
        {
            const char *error_msg = "ERROR: Invalid input format";
            send(client_sock, error_msg, strlen(error_msg), 0);
            close(client_sock);
            return NULL;
        }

        const char *response;
        switch (success)
        {
        case 0:
            response = "SUCCESS: Request successful";
            break;
        case 1:
            response = "ERROR: No such path is accessible on any storage server";
            break;
        case 2:
            response = "ERROR: Naming server failed to send command to storage server";
            break;
        case 3:
            response = "ERROR: No response received from storage server";
            break;
        case 4:
            response = "ERROR: System error occurred on storage server";
            break;
        case 5:
            response = "ERROR: Invalid input format";
            break;
        case 6:
            response = "ERROR: Error in establishing connection between storage servers";
            break;
        default:
            response = "ERROR: Unknown error occurred";
        }

        send(client_sock, response, strlen(response), 0);
    }

    close(client_sock);
    return NULL;
}