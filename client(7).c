#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <errno.h>
#include <signal.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <pwd.h>
#include <grp.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>
#include <errno.h>
#include <stdbool.h>

#define NM_IP "127.0.0.1"
#define NM_PORT 8080
#define BUFFER_SIZE 4096
#define MAX_COMMAND_LENGTH 1024
#define MAX_PATH_LENGTH 256

typedef struct
{
    char ip[16];
    int port;
} ServerInfo;

void handle_read(int sock, const char *path);
void handle_write(int sock, const char *path, int issync);
void handle_info(int sock, const char *path);
void handle_stream(int sock, const char *path);
bool check_mpv_installed(void);

void print_help(void);
void trim_newline(char *str);

ServerInfo get_storage_server(char *arg);

void print_help()
{
    printf("Commands:\n");
    printf("  read <path>    - Read the contents of a file\n");
    printf("  write <path>   - Write text to a file\n");
    printf("  info <path>    - Get information about a file\n");
    printf("  stream <path>  - Stream audio from a file\n");
    printf("  help           - Show this help message\n");
    printf("  exit           - Quit the program\n");
    printf("\n");
}

void trim_newline(char *str)
{
    size_t len = strlen(str);
    if (len > 0 && str[len - 1] == '\n')
    {
        str[len - 1] = '\0';
    }
}

ServerInfo get_storage_server(char *path)
{
    ServerInfo server_info = {0};
    int naming_sock;
    struct sockaddr_in naming_addr;

    naming_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (naming_sock < 0)
    {
        perror("Naming server socket creation failed");
        server_info.port = 0;
        return server_info;
    }

    memset(&naming_addr, 0, sizeof(naming_addr));
    naming_addr.sin_family = AF_INET;
    naming_addr.sin_port = htons(NM_PORT);
    if (inet_pton(AF_INET, NM_IP, &naming_addr.sin_addr) <= 0)
    {
        perror("Invalid naming server IP address");
        close(naming_sock);
        server_info.port = 0;
        return server_info;
    }

    if (connect(naming_sock, (struct sockaddr *)&naming_addr, sizeof(naming_addr)) < 0)
    {
        perror("Naming server connection failed");
        close(naming_sock);
        server_info.port = 0;
        return server_info;
    }

    char request[MAX_COMMAND_LENGTH + MAX_PATH_LENGTH];
    snprintf(request, sizeof(request), "GET_SERVER %s", path);
    printf("Sending request: %s\n", request);

    if (send(naming_sock, request, strlen(request), 0) < 0)
    {
        perror("Failed to send request to naming server");
        close(naming_sock);
        server_info.port = 0;
        return server_info;
    }

    char response[256] = {0};
    int bytes_received = recv(naming_sock, response, sizeof(response) - 1, 0);
    if (bytes_received <= 0)
    {
        perror("Failed to receive response from naming server");
        close(naming_sock);
        server_info.port = 0;
        return server_info;
    }

    response[bytes_received] = '\0';

    if (strncmp(response, "ERROR:", 6) == 0)
    {
        printf("%s\n", response + 7);
        close(naming_sock);
        server_info.port = 0;
        return server_info;
    }

    if (sscanf(response, "%s %d", server_info.ip, &server_info.port) != 2)
    {
        fprintf(stderr, "Invalid response format from naming server\n");
        close(naming_sock);
        server_info.port = 0;
        return server_info;
    }

    printf("Retrieved storage server - IP: %s, Port: %d\n", server_info.ip, server_info.port);
    close(naming_sock);
    return server_info;
}

int main()
{
    char command[MAX_COMMAND_LENGTH];
    char path[MAX_PATH_LENGTH];
    struct sockaddr_in server_addr;
    int current_sock = -1;

    printf("Type 'HELP' for commands, 'EXIT' to QUIT\n\n");

    while (1)
    {
        printf("\n> ");
        if (fgets(command, MAX_COMMAND_LENGTH, stdin) == NULL)
        {
            break;
        }
        trim_newline(command);

        if (strlen(command) == 0)
        {
            continue;
        }

        if (strcmp(command, "EXIT") == 0 || strcmp(command, "QUIT") == 0)
        {
            break;
        }

        if (strcmp(command, "HELP") == 0)
        {
            print_help();
            continue;
        }

        char *cmd = strtok(command, " ");
        char *first_arg = strtok(NULL, " ");
        char *second_arg = strtok(NULL, " ");
        if (cmd == NULL)
        {
            printf("Invalid command. Type 'help' for usage.\n");
            continue;
        }

        if (first_arg == NULL && strcmp(cmd, "HELP") != 0)
        {
            printf("Invalid command. Type 'help' for usage.\n");
            continue;
        }

        if (strcmp(cmd, "CREATE") == 0 || strcmp(cmd, "DELETE") == 0 ||
            strcmp(cmd, "COPY") == 0 || strcmp(cmd, "LISTPATHS") == 0)
        {

            if (strcmp(cmd, "COPY") == 0 && second_arg == NULL)
            {
                printf("COPY command requires source and destination paths.\n");
                continue;
            }

            if (current_sock != -1)
            {
                close(current_sock);
                current_sock = -1;
            }

            int naming_sock = handle_naming_server_command(cmd, first_arg, second_arg);
            if (naming_sock < 0)
            {
                continue;
            }
            close(naming_sock);
            continue;
        }

        printf("%s 1\n",first_arg);
        ServerInfo storage_server = get_storage_server(first_arg);
        if (storage_server.port == 0)
        {
            continue;
        }

        current_sock = socket(AF_INET, SOCK_STREAM, 0);
        if (current_sock < 0)
        {
            perror("Storage server socket creation failed");
            continue;
        }

        memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(storage_server.port);
        inet_pton(AF_INET, storage_server.ip, &server_addr.sin_addr);

        if (connect(current_sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
        {
            perror("Storage server connection failed");
            close(current_sock);
            current_sock = -1;
            continue;
        }

        printf("Connected to storage server\n");

        if (strcmp(cmd, "READ") == 0)
        {
            handle_read(current_sock, first_arg);
        }
        else if (strcmp(cmd, "WRITE") == 0)
        {
            if (second_arg == NULL)
            {
                printf("Invalid command. Type 'help' for usage.\n");
                continue;
            }
            if (second_arg == NULL)
            {
                handle_write(current_sock, first_arg, 0);
            }
            else if (strcmp(second_arg, "--SYNC") == 0)
            {
                handle_write(current_sock, first_arg, 1);
            }
            else
            {
                printf("Unknown command: %s\nType 'help' for usage.\n", cmd);
            }
        }
        else if (strcmp(cmd, "INFO") == 0)
        {
            handle_info(current_sock, first_arg);
        }
        else if (strcmp(cmd, "STREAM") == 0)
        {
            if (!check_mpv_installed())
            {
                printf("Error: mpv player is not installed. Please install it first.\n");
                continue;
            }
            printf("Stream ended\n");
        }
        else
        {
            printf("Unknown command: %s\nType 'help' for usage.\n", cmd);
            continue;
        }
        close(current_sock);
        current_sock = -1;
    }

    if (current_sock != -1)
    {
        close(current_sock);
    }

    printf("Goodbye!\n");
    return 0;
}

void handle_read(int sock, const char *path)
{
    char request[BUFFER_SIZE];
    snprintf(request, sizeof(request), "READ %s", path);

    if (send(sock, request, strlen(request), 0) < 0)
    {
        perror("Send failed");
        return;
    }
    printf("Sent read request\n");

    char size_buffer[BUFFER_SIZE];
    int bytes_received = recv(sock, size_buffer, sizeof(size_buffer) - 1, 0);
    if (bytes_received <= 0)
    {
        perror("Failed to receive file size");
        return;
    }
    size_buffer[bytes_received] = '\0';
    long file_size = atol(size_buffer);

    const char *ack = "READY";
    if (send(sock, ack, strlen(ack), 0) < 0)
    {
        perror("Acknowledgment send failed");
        return;
    }

    printf("\n--- Content of %s ---\n", path);

    char buffer[BUFFER_SIZE];
    long total_bytes_received = 0;
    while ((bytes_received = recv(sock, buffer, sizeof(buffer) - 1, 0)) > 0)
    {
        buffer[bytes_received] = '\0';
        printf("%s", buffer);
        total_bytes_received += bytes_received;

        if (total_bytes_received >= file_size)
        {
            break;
        }
    }

    printf("\n--- End of content ---\n");

    if (bytes_received < 0)
    {
        perror("Read failed");
    }
}

void handle_write(int sock, const char *path, int issync)
{
    char request[BUFFER_SIZE];
    char response[BUFFER_SIZE];

    if (issync == 1)
    {
        snprintf(request, sizeof(request), "WRITE %s --SYNC", path);
    }
    else
    {
        snprintf(request, sizeof(request), "WRITE %s", path);
    }
    printf("Sent write request\n");

    if (send(sock, request, strlen(request), 0) < 0)
    {
        perror("Initial write request failed");
        return;
    }
    if (recv(sock, response, sizeof(response) - 1, 0) <= 0)
    {
        perror("Failed to receive acknowledgment");
        return;
    }

    printf("Enter text to write (Type 'END' on a new line to finish):\n");

    char buffer[BUFFER_SIZE];
    size_t total_size = 0;
    char *accumulated_data = NULL;
    int line_count = 0;

    while (fgets(buffer, sizeof(buffer), stdin) != NULL)
    {
        if (strcmp(buffer, "END\n") == 0)
        {
            break;
        }

        line_count++;
        size_t len = strlen(buffer);
        total_size += len;

        char *temp = realloc(accumulated_data, total_size + 1);
        if (temp == NULL)
        {
            free(accumulated_data);
            perror("Memory allocation failed");
            return;
        }
        accumulated_data = temp;
        memcpy(accumulated_data + total_size - len, buffer, len);
        accumulated_data[total_size] = '\0';
    }

    printf("  - Total lines: %d\n", line_count);
    printf("  - Total size: %zu bytes\n", total_size);

    char size_header[32];
    snprintf(size_header, sizeof(size_header), "SIZE %zu", total_size);

    if (send(sock, size_header, strlen(size_header), 0) < 0)
    {
        free(accumulated_data);
        perror("Failed to send size header");
        return;
    }

    memset(response, 0, sizeof(response));
    if (recv(sock, response, sizeof(response) - 1, 0) <= 0)
    {
        free(accumulated_data);
        perror("Failed to receive size header acknowledgment");
        return;
    }

    size_t bytes_sent = 0;
    int chunk_count = 0;

    while (bytes_sent < total_size)
    {
        chunk_count++;
        size_t chunk_size;
        if (total_size - bytes_sent > BUFFER_SIZE)
        {
            chunk_size = BUFFER_SIZE;
        }
        else
        {
            chunk_size = total_size - bytes_sent;
        }

        if (send(sock, accumulated_data + bytes_sent, chunk_size, 0) < 0)
        {
            free(accumulated_data);
            perror("Failed to send data chunk");
            return;
        }

        memset(response, 0, sizeof(response));
        if (recv(sock, response, sizeof(response) - 1, 0) <= 0)
        {
            free(accumulated_data);
            perror("Failed to receive chunk acknowledgment");
            return;
        }

        bytes_sent += chunk_size;
    }

    free(accumulated_data);

    memset(response, 0, sizeof(response));
    int bytes_received = recv(sock, response, sizeof(response) - 1, 0);
    if (bytes_received > 0)
    {
        response[bytes_received] = '\0';
        printf("%s\n", response);
        return;
    }

    fprintf(stderr, "Failed to receive final server response\n");
    return;
}

void handle_info(int sock, const char *path)
{
    char request[BUFFER_SIZE];
    snprintf(request, sizeof(request), "INFO %s", path);

    if (send(sock, request, strlen(request), 0) < 0)
    {
        perror("Send failed");
        return;
    }

    printf("\n--- File Information for %s ---\n", path);

    char response[BUFFER_SIZE];
    int bytes_received = recv(sock, response, sizeof(response) - 1, 0);
    if (bytes_received > 0)
    {
        response[bytes_received] = '\0';
        printf("--- End of File Information ---\n\n");
        printf("%s", response);
        return;
    }

    printf("Failed to receive file information\n");
    return;
}

int file_exists(const char *path)
{
    struct stat st;
    return stat(path, &st) == 0;
}

bool check_mpv_installed(void)
{
    return system("which mpv >/dev/null 2>&1") == 0;
}

void handle_stream(int sock, const char *path)
{
    char request[BUFFER_SIZE];
    snprintf(request, sizeof(request), "STREAM %s\n", path);

    if (send(sock, request, strlen(request), 0) < 0)
    {
        perror("Send failed");
        return false;
    }

    char size_header[32];
    memset(size_header, 0, sizeof(size_header));
    ssize_t header_received = 0;

    while (header_received < sizeof(size_header) - 1)
    {
        ssize_t n = recv(sock, size_header + header_received, 1, 0);
        if (n <= 0)
        {
            perror("Failed to receive size header");
            return false;
        }
        header_received += n;
        if (size_header[header_received - 1] == '\n')
            break;
    }

    size_t expected_size = 0;
    if (sscanf(size_header, "SIZE:%zu", &expected_size) != 1)
    {
        fprintf(stderr, "Invalid size header received: %s\n", size_header);
        return false;
    }

    int fd = mkstemp(path);
    if (fd < 0)
    {
        perror("Failed to create temp file");
        return false;
    }
    unlink(path);

    char buffer[BUFFER_SIZE];
    size_t total_received = 0;
    bool stream_ended = false;

    while (!stream_ended && total_received < expected_size)
    {
        ssize_t bytes_received = recv(sock, buffer, sizeof(buffer), 0);
        if (bytes_received <= 0)
        {
            perror("Receive error");
            close(fd);
            return false;
        }

        char *end_marker = memmem(buffer, bytes_received, "\nSTREAM_END_MARKER\n", 18);
        size_t bytes_to_write;

        if (end_marker != NULL)
        {
            bytes_to_write = end_marker - buffer;
            stream_ended = true;
        }
        else
        {
            bytes_to_write = bytes_received;
        }

        if (bytes_to_write > 0)
        {
            ssize_t written = write(fd, buffer, bytes_to_write);
            if (written < 0)
            {
                perror("Write to temp file failed");
                close(fd);
                return false;
            }
            total_received += bytes_to_write;
            fflush(stdout);
        }
    }

    lseek(fd, 0, SEEK_SET);

    int pipefd[2];
    if (pipe(pipefd) == -1)
    {
        perror("Pipe creation failed");
        close(fd);
        return false;
    }

    pid_t pid = fork();
    if (pid == -1)
    {
        perror("Fork failed");
        close(pipefd[0]);
        close(pipefd[1]);
        close(fd);
        return false;
    }

    bool success = true;

    if (pid == 0)
    {
        close(pipefd[1]);
        close(fd);

        dup2(pipefd[0], STDIN_FILENO);
        close(pipefd[0]);

        execlp("mpv", "mpv", "-",
               "--no-terminal",
               "--no-cache",
               "--audio-file-auto=no",
               NULL);
        perror("Failed to start mpv");
        exit(1);
    }
    else
    {
        close(pipefd[0]);

        printf("\nPlaying audio...\n");

        while (1)
        {
            ssize_t bytes_read = read(fd, buffer, sizeof(buffer));
            if (bytes_read <= 0)
                break;

            size_t bytes_written = 0;
            while (bytes_written < bytes_read)
            {
                ssize_t written = write(pipefd[1],
                                        buffer + bytes_written,
                                        bytes_read - bytes_written);
                if (written < 0)
                {
                    if (errno == EAGAIN || errno == EWOULDBLOCK)
                    {
                        usleep(1000);
                        continue;
                    }
                    perror("Write to pipe failed");
                    success = false;
                    goto cleanup;
                }
                bytes_written += written;
            }
        }

    cleanup:
        close(pipefd[1]);
        close(fd);

        int status;
        waitpid(pid, &status, 0);

        if (WIFEXITED(status))
        {
            printf("Playback completed successfully\n");
        }
        else
        {
            printf("Playback ended unexpectedly\n");
            success = false;
        }
    }

    return success;
}

int handle_naming_server_command(const char *cmd, const char *first_arg, const char *second_arg)
{
    int naming_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (naming_sock < 0)
    {
        perror("Naming server socket creation failed");
        return -1;
    }

    struct sockaddr_in naming_addr;
    memset(&naming_addr, 0, sizeof(naming_addr));
    naming_addr.sin_family = AF_INET;
    naming_addr.sin_port = htons(NM_PORT);
    inet_pton(AF_INET, NM_IP, &naming_addr.sin_addr);

    if (connect(naming_sock, (struct sockaddr *)&naming_addr, sizeof(naming_addr)) < 0)
    {
        perror("Naming server connection failed");
        close(naming_sock);
        return -1;
    }

    char request[MAX_COMMAND_LENGTH + 2 * MAX_PATH_LENGTH];
    snprintf(request, sizeof(request), "%s %s %s", cmd, first_arg, second_arg);

    printf("Sending command: %s\n", request);

    if (send(naming_sock, request, strlen(request), 0) < 0)
    {
        perror("Failed to send command to naming server");
        close(naming_sock);
        return -1;
    }

    char response[256] = {0};
    int bytes_received = recv(naming_sock, response, sizeof(response) - 1, 0);
    if (bytes_received <= 0)
    {
        perror("Failed to receive response from naming server");
        close(naming_sock);
        return -1;
    }

    response[bytes_received] = '\0';
    printf("%s\n", response);

    return naming_sock;
}
