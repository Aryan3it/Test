// storage_server.c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <pwd.h>
#include <grp.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <dirent.h>
#include <stdbool.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <signal.h>

#define MAX_PATHS 1000
#define NM_IP "127.0.0.1"
#define NM_PORT 8080
#define BUFFER_SIZE 1024
#define PATH_MAX 100000

int nm_sock;
int client_port;
char *accessible_paths[100];
int num_accessible_paths = 0;

void *handle_naming_server_commands(void *arg);
void *handle_naming_server_connections(void *arg);
void *handle_client_requests(void *arg);
void *handle_client_connections(void *arg);

void register_with_naming_server();

void create(char *command);
void delete(char *command);
void copy_file(const char *src_path, const char *dest_path, const char *src_ip, int src_sock, int a);
void copy_directory(const char *src_path, const char *dest_path, const char *src_ip, int src_sock, int a);
void read_file(int client_sock, const char *path);
void write_file(int client_sock, const char *path, int is_sync,
                int naming_server_sock, const char *server_id);
char *get_name_from_path(const char *path);
void get_parent_path(const char *full_path, char *parent_path, size_t size);

void get_file_info(int client_sock, const char *path);
void stream_audio(int client_sock, const char *path);

#define ASYNC_THRESHOLD (100) // 1MB threshold for async writing
typedef struct
{
    char *data;
    size_t size;
    char path[BUFFER_SIZE];
    bool is_sync;
    int naming_server_sock; // Socket for naming server communication
    char server_id[32];     // Storage server identifier
} WriteRequest;

void *async_write_thread(void *arg)
{
    WriteRequest *req = (WriteRequest *)arg;
    printf("Async write thread started for file: %s\n", req->path);
    printf("PATH: %s\n", req->path);
    printf("DATA: %s\n", req->data);
    int fd = open(req->path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0)
    {
        // Send failure notification to naming server
        char error_msg[BUFFER_SIZE];
        snprintf(error_msg, sizeof(error_msg), "WRITE_COMPLETE %s %s FAILED",
                 req->server_id, req->path);
        // send(req->naming_server_sock, error_msg, strlen(error_msg), 0);

        free(req->data);
        free(req);
        return NULL;
    }

    printf("Async write started for file: %s\n", req->path);
    write(fd, req->data, req->size);
    close(fd);
    printf("Async write completed for file: %s\n", req->path);

    // Send success notification to naming server
    char success_msg[BUFFER_SIZE];
    snprintf(success_msg, sizeof(success_msg), "WRITE_COMPLETE %s %s SUCCESS",
             req->server_id, req->path);
    // send(req->naming_server_sock, success_msg, strlen(success_msg), 0);

    // Wait for naming server acknowledgment
    char response[BUFFER_SIZE];
    // recv(req->naming_server_sock, response, sizeof(response) - 1, 0);

    free(req->data);
    free(req);
    return NULL;
}

// int main(int argc, char *argv[])
// {
//     if (argc < 3)
//     {
//         printf("Usage: %s %s <clientport> <accessible_paths>\n", argv[1], argv[2]);
//         return -1;
//     }

//     client_port = atoi(argv[1]);
//     for (int i = 2; i < argc; i++)
//     {
//         accessible_paths[num_accessible_paths++] = argv[2];
//     }

//     register_with_naming_server();

//     pthread_t nm_thread, client_thread;

//     if (pthread_create(&nm_thread, NULL, handle_naming_server_connections, NULL) != 0)
//     {
//         perror("Failed to create naming server thread");
//         exit(EXIT_FAILURE);
//     }

//     if (pthread_create(&client_thread, NULL, handle_client_connections, NULL) != 0)
//     {
//         perror("Failed to create client thread");
//         exit(EXIT_FAILURE);
//     }

//     printf("Storage Server is ready for naming server connections on port %d...\n", NM_PORT);
//     printf("Storage Server is ready for client connections on port %d...\n", client_port);

//     pthread_join(nm_thread, NULL);
//     pthread_join(client_thread, NULL);

//     return 0;
// }

// void register_with_naming_server(void)
// {
//     nm_sock = socket(AF_INET, SOCK_STREAM, 0);
//     struct sockaddr_in serv_addr;

//     serv_addr.sin_family = AF_INET;
//     serv_addr.sin_port = htons(NM_PORT);
//     inet_pton(AF_INET, NM_IP, &serv_addr.sin_addr);

//     if (connect(nm_sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
//     {
//         perror("Failed to connect to Naming Server");
//         close(nm_sock);
//         exit(1);
//     }
//     char indicator[BUFFER_SIZE];
//     snprintf(indicator, sizeof(indicator), "Storage Server");

//     printf("Registering with Naming Server...\n");
//     if (send(nm_sock, indicator, strlen(indicator), 0) < 0)
//     {
//         perror("Failed to send indicator to Naming Server");
//         close(nm_sock);
//         exit(1);
//     }

//     printf("Sent indicator to Naming Server: %s\n", indicator);
//     char registration_info[BUFFER_SIZE];
//     snprintf(registration_info, sizeof(registration_info),
//              "Storage Server: Client Port: %d, Paths: ", client_port);

//     for (int i = 0; i < num_accessible_paths; i++)
//     {
//         strncat(registration_info, accessible_paths[i], sizeof(registration_info) - strlen(registration_info) - 1);
//         if (i < num_accessible_paths - 1)
//         {
//             strncat(registration_info, ", ", sizeof(registration_info) - strlen(registration_info) - 1);
//         }
//     }

//     usleep(1000);
//     printf("%s\n", registration_info);

//     if (send(nm_sock, registration_info, strlen(registration_info), 0) < 0)
//     {
//         perror("Failed to send registration information");
//         close(nm_sock);
//         exit(1);
//     }
//     printf("Sent registration to Naming Server: %s\n", registration_info);

//     char confirmation[BUFFER_SIZE];
//     int bytes_received = recv(nm_sock, confirmation, sizeof(confirmation) - 1, 0);
//     if (bytes_received > 0)
//     {
//         confirmation[bytes_received] = '\0';
//         printf("Received confirmation from Naming Server: %s\n", confirmation);
//         const char *success_msg = "Registration completed";
//         send(nm_sock, success_msg, strlen(success_msg), 0);
//     }
//     else
//     {
//         perror("Failed to receive confirmation from Naming Server");
//         close(nm_sock);
//         exit(1);
//     }
// }
// Helper function to recursively gather files and folders
void gather_paths(const char *base_path)
{
    accessible_paths[num_accessible_paths] = strdup(base_path);
    num_accessible_paths++;
    DIR *dir = opendir(base_path);
    if (dir == NULL)
    {
        perror("Failed to open directory");
        return;
    }

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL)
    {
        // Skip "." and ".."
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
        {
            continue;
        }

        char full_path[BUFFER_SIZE];
        snprintf(full_path, sizeof(full_path), "%s/%s", base_path, entry->d_name);

        struct stat path_stat;
        if (stat(full_path, &path_stat) == 0)
        {
            if (S_ISDIR(path_stat.st_mode))
            {
                // If it's a directory, recurse
                gather_paths(full_path);
            }
            else if (S_ISREG(path_stat.st_mode))
            {
                // If it's a regular file, add it to accessible_paths
                if (num_accessible_paths < MAX_PATHS)
                {
                    accessible_paths[num_accessible_paths] = strdup(full_path);
                    num_accessible_paths++;
                }
                else
                {
                    fprintf(stderr, "Exceeded maximum accessible paths limit!\n");
                    closedir(dir);
                    return;
                }
            }
        }
    }

    closedir(dir);
}

int main(int argc, char *argv[])
{
    if (argc < 3)
    {
        printf("Usage: %s <clientport> <accessible_paths>\n", argv[0]);
        return -1;
    }

    client_port = atoi(argv[1]);
    for (int i = 2; i < argc; i++)
    {
        gather_paths(argv[i]);
    }

    register_with_naming_server();

    pthread_t nm_thread, client_thread;

    if (pthread_create(&nm_thread, NULL, handle_naming_server_connections, NULL) != 0)
    {
        perror("Failed to create naming server thread");
        exit(EXIT_FAILURE);
    }

    if (pthread_create(&client_thread, NULL, handle_client_connections, NULL) != 0)
    {
        perror("Failed to create client thread");
        exit(EXIT_FAILURE);
    }

    printf("Storage Server is ready for naming server connections on port %d...\n", NM_PORT);
    printf("Storage Server is ready for client connections on port %d...\n", client_port);

    pthread_join(nm_thread, NULL);
    pthread_join(client_thread, NULL);

    // Free allocated paths
    for (int i = 0; i < num_accessible_paths; i++)
    {
        free(accessible_paths[i]);
    }

    return 0;
}

// The register_with_naming_server function remains the same
void register_with_naming_server(void)
{
    nm_sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in serv_addr;

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(NM_PORT);
    inet_pton(AF_INET, NM_IP, &serv_addr.sin_addr);

    if (connect(nm_sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        perror("Failed to connect to Naming Server");
        close(nm_sock);
        exit(1);
    }
    char indicator[BUFFER_SIZE];
    snprintf(indicator, sizeof(indicator), "Storage Server");

    printf("Registering with Naming Server...\n");
    if (send(nm_sock, indicator, strlen(indicator), 0) < 0)
    {
        perror("Failed to send indicator to Naming Server");
        close(nm_sock);
        exit(1);
    }

    printf("Sent indicator to Naming Server: %s\n", indicator);
    char registration_info[BUFFER_SIZE];
    snprintf(registration_info, sizeof(registration_info),
             "Storage Server: Client Port: %d, Paths: ", client_port);

    for (int i = 0; i < num_accessible_paths; i++)
    {
        strncat(registration_info, accessible_paths[i], sizeof(registration_info) - strlen(registration_info) - 1);
        if (i < num_accessible_paths - 1)
        {
            strncat(registration_info, ", ", sizeof(registration_info) - strlen(registration_info) - 1);
        }
    }

    usleep(1000);
    printf("%s\n", registration_info);

    if (send(nm_sock, registration_info, strlen(registration_info), 0) < 0)
    {
        perror("Failed to send registration information");
        close(nm_sock);
        exit(1);
    }
    printf("Sent registration to Naming Server: %s\n", registration_info);

    char confirmation[BUFFER_SIZE];
    int bytes_received = recv(nm_sock, confirmation, sizeof(confirmation) - 1, 0);
    if (bytes_received > 0)
    {
        confirmation[bytes_received] = '\0';
        printf("Received confirmation from Naming Server: %s\n", confirmation);
        const char *success_msg = "Registration completed";
        send(nm_sock, success_msg, strlen(success_msg), 0);
    }
    else
    {
        perror("Failed to receive confirmation from Naming Server");
        close(nm_sock);
        exit(1);
    }
}

void *handle_naming_server_connections(void *arg)
{
    char command[BUFFER_SIZE];
    ssize_t bytes_received;

    while (1)
    {
        printf("Storage Server listening for NM commands...\n");

        memset(command, 0, BUFFER_SIZE);

        bytes_received = recv(nm_sock, command, sizeof(command) - 1, 0);

        if (bytes_received <= 0)
        {
            if (bytes_received == 0)
            {
                printf("Connection with naming server closed\n");
            }
            else
            {
                perror("Error receiving data from naming server");
            }
            break; // Exit the loop on connection failure
        }

        command[bytes_received] = '\0';
        printf("Received raw command from Naming Server: %s\n", command);

        // Create a new string for the thread
        char *thread_command = strdup(command);
        if (!thread_command)
        {
            perror("Failed to allocate memory for command");
            continue;
        }

        pthread_t command_thread;
        if (pthread_create(&command_thread, NULL, handle_naming_server_commands, thread_command) != 0)
        {
            perror("Failed to create thread for naming server command");
            free(thread_command);
            continue;
        }
        pthread_detach(command_thread);
    }

    close(nm_sock);
    pthread_exit(NULL);
}

void *handle_naming_server_commands(void *args)
{
    if (!args)
    {
        perror("Error: NULL thread arguments");
        return NULL;
    }

    char *command = (char *)args;
    char type[10] = {0};
    char src_path[BUFFER_SIZE] = {0};
    char dest_path[BUFFER_SIZE] = {0};
    char src_ip[16] = {0};
    int status = 0;

    printf("Received command from Naming Server: %s\n", command);

    if (strncmp(command, "CREATE", 6) == 0)
    {
        printf("CREATE command received\n");
        printf("Command: %s\n", command);
        create(command); // Assuming create() returns int status
        // if (status != 0) {
        //     send(nm_sock, "ERROR: Create operation failed", 28, 0);
        // }
    }
    else if (strncmp(command, "DELETE", 6) == 0)
    {
        printf("DELETE command received\n");
        delete (command); // Assuming delete() returns int status
        // if (status != 0) {
        //     send(nm_sock, "ERROR: Delete operation failed", 28, 0);
        // }
    }
    else if (strncmp(command, "COPY", 4) == 0)
    {
        printf("COPY command received\n");

        // Parse command with bounds checking
        if (sscanf(command, "COPY %9s %1023s %1023s %15s", type, src_path, dest_path, src_ip) != 4)
        {
            send(nm_sock, "ERROR: Invalid COPY command format", 32, 0);
            goto cleanup;
        }

        // Create socket for source connection
        int src_sock = socket(AF_INET, SOCK_STREAM, 0);
        if (src_sock < 0)
        {
            perror("Failed to create socket");

            send(nm_sock, "ERROR: Socket creation failed", 27, 0);
            goto cleanup;
        }

        // Set socket timeout
        struct timeval timeout;
        timeout.tv_sec = 30; // 30 seconds timeout
        timeout.tv_usec = 0;
        if (setsockopt(src_sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0)
        {
            perror("setsockopt failed");
        }

        // Set up connection parameters
        struct sockaddr_in src_addr;
        memset(&src_addr, 0, sizeof(src_addr));
        src_addr.sin_family = AF_INET;
        src_addr.sin_port = htons(client_port);

        if (inet_pton(AF_INET, src_ip, &src_addr.sin_addr) <= 0)
        {
            perror("Invalid IP address");
            char error_msg[BUFFER_SIZE];
            snprintf(error_msg, sizeof(error_msg), "ERROR: Invalid IP address %s", src_ip);
            send(nm_sock, error_msg, strlen(error_msg), 0);
            close(src_sock);
            goto cleanup;
        }

        // Attempt connection
        if (connect(src_sock, (struct sockaddr *)&src_addr, sizeof(src_addr)) < 0)
        {
            perror("Failed to connect to source storage server");
            char error_msg[BUFFER_SIZE];
            snprintf(error_msg, sizeof(error_msg), "ERROR: Failed to connect to source server %s", src_ip);
            send(nm_sock, error_msg, strlen(error_msg), 0);
            close(src_sock);
            goto cleanup;
        }

        // Handle copy based on type
        if (strcmp(type, "FILE") == 0)
        {
            copy_file(src_path, dest_path, src_ip, src_sock, 1);
            // if (status != 0) {
            //     send(nm_sock, "ERROR: File copy failed", 21, 0);
            // }
        }
        else if (strcmp(type, "DIR") == 0)
        {
            copy_directory(src_path, dest_path, src_ip, src_sock, 1);
            // if (status != 0) {
            //     send(nm_sock, "ERROR: Directory copy failed", 26, 0);
            // }
        }
        else
        {
            printf("Unknown type: %s\n", type);
            send(nm_sock, "ERROR: Unknown type", 18, 0);
        }

        close(src_sock);
    }
    else
    {
        printf("Unknown command received: %s\n", command);
        send(nm_sock, "ERROR: Unknown command", 20, 0);
    }

cleanup:
    // Free the command string that was allocated in the parent thread
    free(command);
    return NULL;
}

// Helper function to extract name from path
char *get_name_from_path(const char *path)
{
    char *last_slash = strrchr(path, '/');
    if (last_slash != NULL)
    {
        return last_slash + 1;
    }
    return (char *)path; // Return original path if no slash found
}

// Helper function to get parent directory path
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

void create(char *command)
{
    char type[10], full_path[BUFFER_SIZE];
    sscanf(command, "CREATE %s %s", type, full_path);

    // Extract the name and parent path
    char *name = get_name_from_path(full_path);
    char parent_path[BUFFER_SIZE];
    get_parent_path(full_path, parent_path, sizeof(parent_path));

    // Check if parent directory exists and is accessible
    if (access(parent_path, W_OK) != 0)
    {
        char error_msg[BUFFER_SIZE];
        snprintf(error_msg, sizeof(error_msg), "1 Parent directory %s is not accessible", parent_path);
        send(nm_sock, error_msg, strlen(error_msg), 0);
        return;
    }

    printf("Creating %s: Parent Path: %s, Name: %s\n",
           (strcmp(type, "FILE") == 0) ? "file" : "directory",
           parent_path, name);

    if (strcmp(type, "FILE") == 0)
    {
        char file_path[BUFFER_SIZE];
        snprintf(file_path, sizeof(file_path), "%s/%s", parent_path, name);

        int fd = open(file_path, O_CREAT | O_WRONLY, 0644);
        if (fd < 0)
        {
            perror("Failed to create file");
            char error_msg[BUFFER_SIZE];
            snprintf(error_msg, sizeof(error_msg), "1 Failed to create file %s: %s",
                     name, strerror(errno));
            send(nm_sock, error_msg, strlen(error_msg), 0);
        }
        else
        {
            printf("Created empty file: %s\n", file_path);
            close(fd);
            char success_msg[BUFFER_SIZE];
            snprintf(success_msg, sizeof(success_msg), "0 File created: %s", name);
            send(nm_sock, success_msg, strlen(success_msg), 0);
        }
    }
    else if (strcmp(type, "DIR") == 0)
    {
        char dir_path[BUFFER_SIZE];
        snprintf(dir_path, sizeof(dir_path), "%s/%s", parent_path, name);

        if (mkdir(dir_path, 0755) < 0)
        {
            perror("Failed to create directory");
            char error_msg[BUFFER_SIZE];
            snprintf(error_msg, sizeof(error_msg), "1 Failed to create directory %s: %s",
                     name, strerror(errno));
            send(nm_sock, error_msg, strlen(error_msg), 0);
        }
        else
        {
            printf("Created directory: %s\n", dir_path);
            char success_msg[BUFFER_SIZE];
            snprintf(success_msg, sizeof(success_msg), "0 Directory created: %s", name);
            send(nm_sock, success_msg, strlen(success_msg), 0);
            accessible_paths[num_accessible_paths++] = dir_path;
        }
    }
    else
    {
        printf("Unknown type: %s\n", type);
        send(nm_sock, "2 Unknown type", 18, 0);
    }
}

void delete(char *command)
{
    char type[10], full_path[BUFFER_SIZE];
    sscanf(command, "DELETE %s %s", type, full_path);

    // Extract the name and parent path
    char *name = get_name_from_path(full_path);
    char parent_path[BUFFER_SIZE];
    get_parent_path(full_path, parent_path, sizeof(parent_path));

    // Check if parent directory exists and is accessible
    if (access(parent_path, W_OK) != 0)
    {
        char error_msg[BUFFER_SIZE];
        snprintf(error_msg, sizeof(error_msg), "1 Parent directory %s is not accessible", parent_path);
        send(nm_sock, error_msg, strlen(error_msg), 0);
        return;
    }

    printf("Deleting %s: Parent Path: %s, Name: %s\n",
           (strcmp(type, "FILE") == 0) ? "file" : "directory",
           parent_path, name);

    if (strcmp(type, "FILE") == 0)
    {
        char file_path[BUFFER_SIZE];
        snprintf(file_path, sizeof(file_path), "%s/%s", parent_path, name);

        // Check if file exists and is accessible
        if (access(file_path, F_OK) != 0)
        {
            char error_msg[BUFFER_SIZE];
            snprintf(error_msg, sizeof(error_msg), "1 File %s not found", name);
            send(nm_sock, error_msg, strlen(error_msg), 0);
            return;
        }

        if (unlink(file_path) < 0)
        {
            perror("Failed to delete file");
            char error_msg[BUFFER_SIZE];
            snprintf(error_msg, sizeof(error_msg), "1 Failed to delete file %s: %s",
                     name, strerror(errno));
            send(nm_sock, error_msg, strlen(error_msg), 0);
        }
        else
        {
            printf("Deleted file: %s\n", file_path);
            char success_msg[BUFFER_SIZE];
            snprintf(success_msg, sizeof(success_msg), "0 File deleted: %s", name);
            send(nm_sock, success_msg, strlen(success_msg), 0);
        }
    }
    else if (strcmp(type, "DIR") == 0)
    {
        char dir_path[BUFFER_SIZE];
        snprintf(dir_path, sizeof(dir_path), "%s/%s", parent_path, name);

        // Check if directory exists and is accessible
        if (access(dir_path, F_OK) != 0)
        {
            char error_msg[BUFFER_SIZE];
            snprintf(error_msg, sizeof(error_msg), "1 Directory %s not found", name);
            send(nm_sock, error_msg, strlen(error_msg), 0);
            return;
        }

        if (rmdir(dir_path) < 0)
        {
            perror("Failed to delete directory");
            char error_msg[BUFFER_SIZE];
            snprintf(error_msg, sizeof(error_msg), "1 Failed to delete directory %s: %s",
                     name, strerror(errno));
            send(nm_sock, error_msg, strlen(error_msg), 0);
        }
        else
        {
            printf("Deleted directory: %s\n", dir_path);
            char success_msg[BUFFER_SIZE];
            snprintf(success_msg, sizeof(success_msg), "0 Directory deleted: %s", name);
            send(nm_sock, success_msg, strlen(success_msg), 0);
        }
    }
    else
    {
        printf("Unknown type: %s\n", type);
        send(nm_sock, "2 Unknown type", 18, 0);
    }
}

// void create(char *command)
// {
//     char type[10], path[BUFFER_SIZE];
//     sscanf(command, "CREATE %s %s", type, path);

//     if (strcmp(type, "FILE") == 0)
//     {
//         int fd = open(path, O_CREAT | O_WRONLY, 0644);
//         if (fd < 0)
//         {
//             perror("Failed to create file");
//             char error_msg[BUFFER_SIZE];
//             snprintf(error_msg, sizeof(error_msg), "1 Failed to create file %s", path);
//             send(nm_sock, error_msg, strlen(error_msg), 0);
//         }
//         else
//         {
//             printf("Created empty file at path: %s\n", path);
//             close(fd);
//             char success_msg[BUFFER_SIZE];
//             snprintf(success_msg, sizeof(success_msg), "0 File created at %s", path);
//             send(nm_sock, success_msg, strlen(success_msg), 0);
//         }
//     }
//     else if (strcmp(type, "DIR") == 0)
//     {
//         if (mkdir(path, 0755) < 0)
//         {
//             perror("Failed to create directory");
//             char error_msg[BUFFER_SIZE];
//             snprintf(error_msg, sizeof(error_msg), "1 Failed to create directory %s", path);
//             send(nm_sock, error_msg, strlen(error_msg), 0);
//         }
//         else
//         {
//             printf("Created directory at path: %s\n", path);
//             char success_msg[BUFFER_SIZE];
//             snprintf(success_msg, sizeof(success_msg), "0 Directory created at %s", path);
//             send(nm_sock, success_msg, strlen(success_msg), 0);
//         }
//     }
//     else
//     {
//         printf("Unknown type: %s\n", type);
//         send(nm_sock, "2 Unknown type", 18, 0);
//     }
// }

// void delete(char *command)
// {
//     char type[10], path[BUFFER_SIZE];
//     sscanf(command, "DELETE %s %s", type, path);
//     if (strcmp(type, "FILE") == 0)
//     {
//         if (unlink(path) < 0)
//         {
//             perror("Failed to delete file");
//             char error_msg[BUFFER_SIZE];
//             snprintf(error_msg, sizeof(error_msg), "1 Failed to delete file %s", path);
//             send(nm_sock, error_msg, strlen(error_msg), 0);
//         }
//         else
//         {
//             printf("Deleted file at path: %s\n", path);
//             char success_msg[BUFFER_SIZE];
//             snprintf(success_msg, sizeof(success_msg), "0 File deleted at %s", path);
//             send(nm_sock, success_msg, strlen(success_msg), 0);
//         }
//     }
//     else if (strcmp(type, "DIR") == 0)
//     {
//         if (rmdir(path) < 0)
//         {
//             perror("Failed to delete directory");
//             char error_msg[BUFFER_SIZE];
//             snprintf(error_msg, sizeof(error_msg), "1 Failed to delete directory %s", path);
//             send(nm_sock, error_msg, strlen(error_msg), 0);
//         }
//         else
//         {
//             printf("Deleted directory at path: %s\n", path);
//             char success_msg[BUFFER_SIZE];
//             snprintf(success_msg, sizeof(success_msg), "0 Directory deleted at %s", path);
//             send(nm_sock, success_msg, strlen(success_msg), 0);
//         }
//     }
//     else
//     {
//         printf("Unknown type: %s\n", type);
//         send(nm_sock, "2 Unknown type", 18, 0);
//     }
// }

// void copy(char *command)
// {
//     char src_path[512], dest_path[512], src_ip[20];
//     int src_port;

//     sscanf(command, "COPY %s %d %s %s", src_ip, &src_port, src_path, dest_path);

//     int source_sock = socket(AF_INET, SOCK_STREAM, 0);
//     struct sockaddr_in source_addr;
//     source_addr.sin_family = AF_INET;
//     source_addr.sin_port = htons(src_port);
//     inet_pton(AF_INET, src_ip, &source_addr.sin_addr);

//     if (connect(source_sock, (struct sockaddr *)&source_addr, sizeof(source_addr)) < 0)
//     {
//         perror("Failed to connect to source storage server");
//         send(nm_sock, "3 Connection to source failed", 32, 0);
//         return NULL;
//     }

//     char transfer_cmd[BUFFER_SIZE];
//     snprintf(transfer_cmd, sizeof(transfer_cmd), "SEND_FILE %s %s", src_path, dest_path);
//     send(source_sock, transfer_cmd, strlen(transfer_cmd), 0);

//     int dest_fd = open(dest_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
//     if (dest_fd < 0)
//     {
//         perror("Failed to create destination file");
//         send(nm_sock, "ERROR: Failed to create destination file", 37, 0);
//         close(source_sock);
//         return NULL;
//     }

//     char buffer[BUFFER_SIZE];
//     ssize_t bytes_read;
//     while ((bytes_read = recv(source_sock, buffer, BUFFER_SIZE, 0)) > 0)
//     {
//         write(dest_fd, buffer, bytes_read);
//     }

//     close(dest_fd);
//     close(source_sock);
//     send(nm_sock, "SUCCESS: File copied successfully", 31, 0);
// }
void copy_directory(const char *src_path, const char *dest_path, const char *src_ip, int src_sock, int a)
{
    printf("copy started\n");

    // Create destination directory
    if (mkdir(dest_path, 0755) < 0 && errno != EEXIST)
    {
        perror("Failed to create destination directory");
        char error_msg[BUFFER_SIZE];
        snprintf(error_msg, sizeof(error_msg), "1 Failed to create destination directory %s", dest_path);
        send(nm_sock, error_msg, strlen(error_msg), 0);
        return;
    }

    printf("directory created\n");

    // Connect to source storage server

    // Request directory listing from source server
    char request[BUFFER_SIZE];
    snprintf(request, sizeof(request), "LIST %s", src_path);
    send(src_sock, request, strlen(request), 0);

    // Read directory listing
    char buffer[BUFFER_SIZE];
    char message[BUFFER_SIZE];
    ssize_t bytes_received = recv(src_sock, buffer, sizeof(buffer) - 1, 0);
    if (bytes_received <= 0)
    {
        perror("Failed to receive directory listing");
        snprintf(message, sizeof(message), "2 Failed to receive data from source server %s", src_ip);
        close(src_sock);
        return;
    }
    buffer[bytes_received] = '\0';

    // Make a copy of the buffer
    char copy[BUFFER_SIZE];
    strcpy(copy, buffer);

    // Parse directory listing and copy each item
    char *line = strtok(buffer, "\n");
    int b = 0;
    while (line != NULL)
    {
        // Make a copy of the line
        char line_copy[256];
        strcpy(line_copy, line);

        printf("%s\n", line_copy);
        char item_type[10], item_name[256];
        sscanf(line, "%s %s", item_type, item_name);
        char src_item_path[BUFFER_SIZE], dest_item_path[BUFFER_SIZE];
        snprintf(src_item_path, sizeof(src_item_path), "%s/%s", src_path, item_name);
        snprintf(dest_item_path, sizeof(dest_item_path), "%s/%s", dest_path, item_name);

        if (strcmp(item_type, "FILE") == 0)
        {
            printf("file copy started\n");
            copy_file(src_item_path, dest_item_path, src_ip, src_sock, 0);
            printf("file copy finished\n");
        }
        else if (strcmp(item_type, "DIR") == 0)
        {
            printf("on dir\n");
            copy_directory(src_item_path, dest_item_path, src_ip, src_sock, 0);
            printf("out of dir\n");
        }
        printf("%s\n\n", copy);
        strcpy(buffer, copy);
        line = strtok(buffer, "\n");
        b++;
        for (int i = 0; i < b; i++)
        {
            line = strtok(NULL, "\n");
        }
    }

    printf("%s\n\n", copy);

    printf("directory copy finish\n");
    if (a)
    {
        close(src_sock);
        char success_msg[BUFFER_SIZE];
        snprintf(success_msg, sizeof(success_msg), "0 Directory copied from %s to %s", src_path, dest_path);
        send(nm_sock, success_msg, strlen(success_msg), 0);
    }
}
void copy_file(const char *src_path, const char *dest_path, const char *src_ip, int src_sock, int a)
{
    printf("copy started\n");
    printf("connected\n");
    // Request file size from source server
    char request[BUFFER_SIZE];
    snprintf(request, sizeof(request), "READ %s", src_path);
    send(src_sock, request, strlen(request), 0);
    char message[BUFFER_SIZE];

    // Receive file size
    char size_buffer[BUFFER_SIZE];
    ssize_t bytes_received = recv(src_sock, size_buffer, sizeof(size_buffer) - 1, 0);
    if (bytes_received <= 0)
    {
        perror("Failed to receive file size");
        snprintf(message, sizeof(message), "2 Failed to receive data from source server %s", src_ip);
        close(src_sock);
        return;
    }
    size_buffer[bytes_received] = '\0';
    long file_size = atol(size_buffer);
    printf("%d size received\n", file_size);

    snprintf(request, sizeof(request), "Received size");
    send(src_sock, request, strlen(request), 0);

    // Create destination file
    int dest_fd = open(dest_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (dest_fd < 0)
    {
        perror("Failed to create destination file");
        char error_msg[BUFFER_SIZE];
        snprintf(error_msg, sizeof(error_msg), "1 Failed to create destination file %s", dest_path);
        send(nm_sock, error_msg, strlen(error_msg), 0);
        close(src_sock);
        return;
    }
    printf("file created\n");

    // Copy data
    char buffer[BUFFER_SIZE];
    long bytes_left = file_size;
    while (bytes_left > 0)
    {
        ssize_t bytes_to_read = (bytes_left > sizeof(buffer)) ? sizeof(buffer) : bytes_left;
        ssize_t bytes_received = recv(src_sock, buffer, bytes_to_read, 0);
        if (bytes_received <= 0)
        {
            perror("Error receiving data from source server");
            char error_msg[BUFFER_SIZE];
            snprintf(error_msg, sizeof(error_msg), "2 Failed to receive data from source server %s", src_ip);
            send(nm_sock, error_msg, strlen(error_msg), 0);
            close(src_sock);
            close(dest_fd);
            return;
        }

        if (write(dest_fd, buffer, bytes_received) != bytes_received)
        {
            perror("1 Failed to write to destination file");
            close(src_sock);
            close(dest_fd);
            return;
        }

        bytes_left -= bytes_received;
    }

    close(dest_fd);

    if (a)
    {
        close(src_sock);
        char success_msg[BUFFER_SIZE];
        snprintf(success_msg, sizeof(success_msg), "0 File copied from %s to %s", src_path, dest_path);
        send(nm_sock, success_msg, strlen(success_msg), 0);
    }
}

void *handle_client_connections(void *arg)
{
    int client_sock, server_sock;
    struct sockaddr_in server_addr, cli_addr;

    server_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock < 0)
    {
        perror("Failed to create client socket");
        pthread_exit(NULL);
    }

    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(client_port);

    if (bind(server_sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        perror("Failed to bind client socket");
        close(server_sock);
        pthread_exit(NULL);
    }

    listen(server_sock, 5);

    while (1)
    {
        socklen_t client_len = sizeof(cli_addr);
        client_sock = accept(server_sock, (struct sockaddr *)&cli_addr, &client_len);
        if (client_sock < 0)
        {
            perror("Failed to accept client connection");
            continue;
        }

        printf("Accepted connection from Client\n");

        int *client_sock_ptr = malloc(sizeof(int));
        if (!client_sock_ptr)
        {
            perror("Failed to allocate memory for client socket pointer");
            close(client_sock);
            continue;
        }
        *client_sock_ptr = client_sock;

        pthread_t client_thread;
        pthread_create(&client_thread, NULL, handle_client_requests, client_sock_ptr);
        pthread_detach(client_thread);
    }

    close(server_sock);
    pthread_exit(NULL);
}

void *handle_client_requests(void *arg)
{
    int client_sock = *(int *)arg;

    char command[BUFFER_SIZE];
    ssize_t bytes_received;

    printf("Storage Server listening for client requests...\n");

    while ((bytes_received = recv(client_sock, command, sizeof(command) - 1, 0)) > 0)
    {
        command[bytes_received] = '\0';
        printf("Received client command: %s\n", command);

        if (strncmp(command, "READ", 4) == 0)
        {
            char path[BUFFER_SIZE];
            sscanf(command, "READ %s", path);
            read_file(client_sock, path);
        }
        else if (strncmp(command, "WRITE", 5) == 0)
        {
            char path[BUFFER_SIZE];
            int is_sync = 0;
            char *sync_flag = strstr(command, "--SYNC");
            if (sync_flag)
            {
                is_sync = 1;
                *sync_flag = '\0'; // Truncate string at flag
            }
            sscanf(command, "WRITE %s", path);

            write_file(client_sock, path, is_sync, nm_sock, client_port);
        }
        else if (strncmp(command, "INFO", 4) == 0)
        {
            char path[BUFFER_SIZE];
            sscanf(command, "INFO %s", path);
            get_file_info(client_sock, path);
        }
        else if (strncmp(command, "STREAM", 6) == 0)
        {
            char path[BUFFER_SIZE];
            sscanf(command, "STREAM %s", path);
            stream_audio(client_sock, path);
        }
        else
        {
            char error_msg[] = "6 Unknown command\n";
            send(client_sock, error_msg, strlen(error_msg), 0);
        }
    }

    if (bytes_received <= 0)
    {
        perror("Client disconnected");
    }

    close(client_sock);
    return NULL;
}

void read_file(int client_sock, const char *path)
{
    int fd = open(path, O_RDONLY);
    if (fd < 0)
    {
        perror("Failed to open file for reading");
        char error_msg[] = "ERROR: Failed to read file\n";
        send(client_sock, error_msg, strlen(error_msg), 0);
        return;
    }
    struct stat st;
    if (fstat(fd, &st) < 0)
    {
        perror("Failed to get file size");
        char error_msg[] = "ERROR: Failed to get file size\n";
        send(client_sock, error_msg, strlen(error_msg), 0);
        close(fd);
        return;
    }
    long file_size = st.st_size;

    char size_buffer[BUFFER_SIZE];
    snprintf(size_buffer, sizeof(size_buffer), "%ld", file_size);
    send(client_sock, size_buffer, strlen(size_buffer), 0);

    // Wait for client's response
    char client_response[BUFFER_SIZE];
    ssize_t bytes_received = recv(client_sock, client_response, sizeof(client_response) - 1, 0);
    if (bytes_received <= 0)
    {
        perror("Failed to receive client response");
        close(fd);
        return;
    }
    client_response[bytes_received] = '\0';

    char buffer[BUFFER_SIZE];
    ssize_t bytes_read;
    while ((bytes_read = read(fd, buffer, sizeof(buffer))) > 0)
    {
        send(client_sock, buffer, bytes_read, 0);
    }

    const char *end_marker = "EOF_MARKER";
    send(client_sock, end_marker, strlen(end_marker), 0);

    close(fd);
    printf("Sent file contents of %s to client\n", path);
}

void write_file(int client_sock, const char *path, int is_sync, int naming_server_sock, const char *server_id)
{
    printf("\n[INFO] Starting write_file operation for path: %s\n", path);
    printf("[INFO] Mode: %s\n", is_sync ? "synchronous" : "asynchronous");

    if (!path || !server_id)
    {
        fprintf(stderr, "[ERROR] Invalid parameters: path or server_id is NULL\n");
        return;
    }

    char response[BUFFER_SIZE] = {0};
    const char *ack = "ACK";

    if (client_sock < 0)
    {
        fprintf(stderr, "[ERROR] Invalid client socket: %d\n", client_sock);
        return;
    }

    printf("[INFO] Sending initial acknowledgment...\n");
    if (send(client_sock, ack, strlen(ack), 0) < 0)
    {
        perror("[ERROR] Failed to send initial ACK");
        return;
    }
    printf("[SUCCESS] Initial acknowledgment sent\n");

    // Receive the size header
    char size_str[32] = {0};
    printf("[INFO] Waiting to receive size header...\n");
    ssize_t bytes_received = recv(client_sock, size_str, sizeof(size_str) - 1, 0);
    if (bytes_received <= 0)
    {
        perror("[ERROR] Failed to receive size header");
        return;
    }

    size_str[bytes_received] = '\0';
    size_t total_size = 0;
    if (sscanf(size_str, "SIZE %zu", &total_size) != 1)
    {
        fprintf(stderr, "[ERROR] Invalid size format received: %s\n", size_str);
        return;
    }

    if (total_size == 0)
    {
        fprintf(stderr, "[ERROR] Invalid file size: %zu\n", total_size);
        return;
    }

    printf("[INFO] File transfer details:\n");
    printf("  - Header received: %s\n", size_str);
    printf("  - Total size: %zu bytes\n", total_size);
    printf("  - Path: %s\n", path);

    // Send acknowledgment for size header
    printf("[INFO] Sending size acknowledgment...\n");
    if (send(client_sock, ack, strlen(ack), 0) < 0)
    {
        perror("[ERROR] Failed to send size ACK");
        return;
    }
    printf("[SUCCESS] Size acknowledgment sent\n");

    // Determine if we should use async write
    bool use_async = !is_sync && (total_size > ASYNC_THRESHOLD);
    printf("[INFO] Write mode determination:\n");
    printf("  - Async threshold: %d bytes\n", ASYNC_THRESHOLD);
    printf("  - Selected mode: %s\n", use_async ? "async" : "sync");

    WriteRequest *req = NULL;
    if (use_async)
    {
        printf("[INFO] Initializing async write request...\n");
        req = calloc(1, sizeof(WriteRequest));
        if (!req)
        {
            perror("[ERROR] Failed to allocate WriteRequest");
            return;
        }

        req->data = malloc(total_size + 1); // +1 for null terminator
        if (!req->data)
        {
            perror("[ERROR] Failed to allocate data buffer");
            free(req);
            return;
        }
        req->size = total_size;
        strncpy(req->path, path, BUFFER_SIZE - 1);
        req->path[BUFFER_SIZE - 1] = '\0';
        req->is_sync = is_sync;
        printf("[SUCCESS] Async write request initialized\n");
    }

    int fd = -1;
    if (!use_async)
    {
        printf("[INFO] Opening file for synchronous writing: %s\n", path);
        fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0)
        {
            perror("[ERROR] Failed to open file for writing");
            char error_msg[] = "ERROR: Failed to write to file\n";
            send(client_sock, error_msg, strlen(error_msg), 0);
            return;
        }
        printf("[SUCCESS] File opened successfully\n");
    }

    char buffer[BUFFER_SIZE];
    size_t total_received = 0;
    int chunk_count = 0;

    printf("[INFO] Starting data reception loop\n");
    while (total_received < total_size)
    {
        memset(buffer, 0, sizeof(buffer));
        ssize_t chunk_size = sizeof(buffer) - 1;
        if (total_received + chunk_size > total_size)
        {
            chunk_size = total_size - total_received;
        }

        printf("[INFO] Receiving chunk %d (expecting %zd bytes)...\n", ++chunk_count, chunk_size);
        bytes_received = recv(client_sock, buffer, chunk_size, 0);

        if (bytes_received <= 0)
        {
            fprintf(stderr, "[ERROR] Connection error or client disconnected during chunk %d\n", chunk_count);
            break;
        }

        if (use_async && req && req->data)
        {
            memcpy((char *)req->data + total_received, buffer, bytes_received);
        }
        else if (fd != -1)
        {
            ssize_t bytes_written = write(fd, buffer, bytes_received);
            if (bytes_written < 0)
            {
                perror("[ERROR] Write failed");
                break;
            }
            printf("[SUCCESS] Wrote %zd bytes to file\n", bytes_written);
        }

        total_received += bytes_received;
        printf("[INFO] Progress: %zu/%zu bytes (%.1f%%)\n",
               total_received, total_size,
               (float)total_received / total_size * 100);

        // Send acknowledgment for data chunk
        if (send(client_sock, ack, strlen(ack), 0) < 0)
        {
            perror("[ERROR] Failed to send data chunk ACK");
            break;
        }
    }

    if (total_received != total_size)
    {
        fprintf(stderr, "[ERROR] Incomplete file transfer:\n");
        fprintf(stderr, "  - Received: %zu bytes\n", total_received);
        fprintf(stderr, "  - Expected: %zu bytes\n", total_size);
        if (use_async && req)
        {
            free(req->data);
            free(req);
        }
        if (fd != -1)
        {
            close(fd);
        }
        return;
    }

    printf("[SUCCESS] File transfer complete: %zu bytes received\n", total_received);

    if (use_async && req && req->data)
    {
        req->data[total_size] = '\0';
        printf("[INFO] Starting async write thread...\n");
        pthread_t write_thread;
        if (pthread_create(&write_thread, NULL, async_write_thread, req) != 0)
        {
            perror("[ERROR] Failed to create async write thread");
            free(req->data);
            free(req);
            return;
        }
        pthread_detach(write_thread);

        const char *async_msg = "Request accepted for async writing\n";
        send(client_sock, async_msg, strlen(async_msg), 0);
        printf("[SUCCESS] Async write thread started successfully\n");
    }
    else
    {
        if (fd != -1)
        {
            close(fd);
            const char *sync_msg = "File written successfully (sync)\n";
            send(client_sock, sync_msg, strlen(sync_msg), 0);
            printf("[SUCCESS] Synchronous write completed successfully\n");
        }
    }

    printf("[INFO] Write operation completed for %s\n\n", path);
}

void stream_audio(int client_sock, const char *path)
{
    int fd = open(path, O_RDONLY);
    if (fd < 0)
    {
        perror("Failed to open audio file for streaming");
        char error_msg[] = "ERROR: Failed to stream audio file\n";
        send(client_sock, error_msg, strlen(error_msg), 0);
        return;
    }

    struct stat st;
    if (fstat(fd, &st) < 0)
    {
        perror("Failed to get file stats");
        close(fd);
        return;
    }

    char size_header[32];
    snprintf(size_header, sizeof(size_header), "SIZE:%ld\n", st.st_size);
    if (send(client_sock, size_header, strlen(size_header), 0) < 0)
    {
        perror("Failed to send size header");
        close(fd);
        return;
    }

    char buffer[BUFFER_SIZE];
    ssize_t bytes_read;
    size_t total_sent = 0;

    while ((bytes_read = read(fd, buffer, sizeof(buffer))) > 0)
    {
        ssize_t bytes_sent = send(client_sock, buffer, bytes_read, MSG_NOSIGNAL);
        if (bytes_sent < 0)
        {
            if (errno == EPIPE)
            {
                printf("Client disconnected\n");
                break;
            }
            perror("Send failed");
            break;
        }
        total_sent += bytes_sent;
        printf("\rSent: %zu bytes of %ld", total_sent, st.st_size);
        fflush(stdout);
    }

    if (total_sent == st.st_size)
    {
        const char *end_marker = "\nSTREAM_END_MARKER\n";
        send(client_sock, end_marker, strlen(end_marker), MSG_NOSIGNAL);
        printf("\nStream completed successfully\n");
    }
    else
    {
        printf("\nStream ended unexpectedly\n");
    }

    close(fd);
}

void get_file_info(int client_sock, const char *path)
{
    struct stat file_stat;
    if (lstat(path, &file_stat) < 0)
    {
        char error_msg[BUFFER_SIZE];
        snprintf(error_msg, sizeof(error_msg), "ERROR: Could not retrieve file info for %s: %s\n",
                 path, strerror(errno));
        send(client_sock, error_msg, strlen(error_msg), 0);
        return;
    }

    struct passwd *pw = getpwuid(file_stat.st_uid);
    struct group *gr = getgrgid(file_stat.st_gid);

    char access_time[30], mod_time[30], change_time[30];

    struct tm *tm_info_a = localtime(&file_stat.st_atime);
    strftime(access_time, sizeof(access_time), "%Y-%m-%d %H:%M:%S", tm_info_a);
    struct tm *tm_info_m = localtime(&file_stat.st_mtime);
    strftime(mod_time, sizeof(mod_time), "%Y-%m-%d %H:%M:%S", tm_info_m);
    struct tm *tm_info_c = localtime(&file_stat.st_ctime);
    strftime(change_time, sizeof(change_time), "%Y-%m-%d %H:%M:%S", tm_info_c);

    char perms[11];
    snprintf(perms, 11, "%c%c%c%c%c%c%c%c%c%c",
             (file_stat.st_mode & S_IRUSR) ? 'r' : '-',
             (file_stat.st_mode & S_IWUSR) ? 'w' : '-',
             (file_stat.st_mode & S_IXUSR) ? 'x' : '-',
             (file_stat.st_mode & S_IRGRP) ? 'r' : '-',
             (file_stat.st_mode & S_IWGRP) ? 'w' : '-',
             (file_stat.st_mode & S_IXGRP) ? 'x' : '-',
             (file_stat.st_mode & S_IROTH) ? 'r' : '-',
             (file_stat.st_mode & S_IWOTH) ? 'w' : '-',
             (file_stat.st_mode & S_IXOTH) ? 'x' : '-',
             '\0');

    char *file_type[30];
    if (S_ISREG(file_stat.st_mode))
    {
        strcpy(file_type, "Regular File");
    }
    else if (S_ISDIR(file_stat.st_mode))
    {
        strcpy(file_type, "Directory");
    }
    else if (S_ISLNK(file_stat.st_mode))
    {
        strcpy(file_type, "Symbolic Link");
    }
    else if (S_ISBLK(file_stat.st_mode))
    {
        strcpy(file_type, "Block Device");
    }
    else if (S_ISCHR(file_stat.st_mode))
    {
        strcpy(file_type, "Character Device");
    }
    else if (S_ISFIFO(file_stat.st_mode))
    {
        strcpy(file_type, "FIFO/Pipe");
    }
    else if (S_ISSOCK(file_stat.st_mode))
    {
        strcpy(file_type, "Socket");
    }
    else
    {
        strcpy(file_type, 'Unknown');
    }

    char size_str[20];
    if (file_stat.st_size < 1024)
    {
        snprintf(size_str, sizeof(size_str), "%ld B", file_stat.st_size);
    }
    else if (file_stat.st_size < 1024 * 1024)
    {
        snprintf(size_str, sizeof(size_str), "%.2f KB", file_stat.st_size / 1024.0);
    }
    else if (file_stat.st_size < 1024 * 1024 * 1024)
    {
        snprintf(size_str, sizeof(size_str), "%.2f MB", file_stat.st_size / (1024.0 * 1024.0));
    }
    else
    {
        snprintf(size_str, sizeof(size_str), "%.2f GB", file_stat.st_size / (1024.0 * 1024.0 * 1024.0));
    }

    char info[BUFFER_SIZE];
    snprintf(info, sizeof(info),
             "File: %s\n"
             "Type: %s\n"
             "Size: %s (%ld bytes)\n"
             "Permissions: %s (Octal: %o)\n"
             "Owner: %s (%d)\n"
             "Group: %s (%d)\n"
             "Device ID: %ld\n"
             "Inode: %ld\n"
             "Links: %ld\n"
             "Access Time: %s\n"
             "Modify Time: %s\n"
             "Change Time: %s\n"
             "Block Size: %ld\n"
             "Blocks Allocated: %ld\n",
             path,
             file_type,
             size_str, file_stat.st_size,
             perms, file_stat.st_mode & 0777,
             pw ? pw->pw_name : "unknown", file_stat.st_uid,
             gr ? gr->gr_name : "unknown", file_stat.st_gid,
             (long)file_stat.st_dev,
             (long)file_stat.st_ino,
             (long)file_stat.st_nlink,
             access_time,
             mod_time,
             change_time,
             (long)file_stat.st_blksize,
             (long)file_stat.st_blocks);

    if (S_ISLNK(file_stat.st_mode))
    {
        char link_target[PATH_MAX];
        ssize_t len = readlink(path, link_target, sizeof(link_target) - 1);
        if (len != -1)
        {
            link_target[len] = '\0';
            char link_info[BUFFER_SIZE];
            snprintf(link_info, sizeof(link_info), "Link Target: %s\n", link_target);
            strncat(info, link_info, sizeof(info) - strlen(info) - 1);
        }
    }

    send(client_sock, info, strlen(info), 0);
    printf("Sent file info for %s to client\n", path);
}