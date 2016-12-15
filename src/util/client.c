#define _XOPEN_SOURCE
/**
 * client.c
 *  CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <errno.h>

#include "util/const.h"
#include "util/message.h"
#include "util/log.h"
#include "util/strmanip.h"

#define DEFAULT_STDIN_BUFFER_SIZE 1024

/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/
int connect_client() {
    int client_socket;
    size_t len;
    struct sockaddr_un remote;

    log_info("-- Attempting to connect...\n");

    if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("-- %d: Failed to create socket.");
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
    if (connect(client_socket, (struct sockaddr *)&remote, len) == -1) {
        perror("-- Client connect failed.\n");
        return -1;
    }

    log_info("-- Client connected at socket: %d.\n", client_socket);
    return client_socket;
}

void sendMessage(message send_message, int socket) {
    if (send(socket, &send_message, sizeof(message), 0) == -1) {
        log_err("-- Unable to send message metadata\n");
        exit(1);
    } 
    if (send(socket, send_message.payload, send_message.length, 0) == -1) {
        log_err("-- Unable to send message content\n");
        exit(1);
    }
}

void receiveMessage(int socket) {
    message recv_message;
    int len = 0;

    // retrieve response from server
    if ((len = recv(socket, &recv_message, sizeof(message), 0)) <= 0) {
        if (len < 0)
            log_err("-- Failed to receive message.\n");
        else {
            log_info("-- Server closed connection!\n");
            exit(0);
        }
    }
    
    // handle server response
    log_info("-- Client recv_message: status %i, length %i\n", recv_message.status, (int) recv_message.length);
    if (recv_message.status == OK_WAIT_FOR_RESPONSE && (int) recv_message.length > 0) {
        // Calculate number of bytes in response package
        int num_bytes = (int) recv_message.length;
        char payload[num_bytes + 1];

        // Receive the payload and print it out
        if ((len = recv(socket, payload, num_bytes, 0)) > 0) {
            payload[num_bytes] = '\0';
            printf("%s", payload);
        }
    }
}

void handleLoadQuery(char* query, int socket) {
    // extract message path
    char* path = query + 5;
    path = trim_whitespace(path);
    path = trim_quotes(path);
    size_t len = strlen(path);
    if (path[len - 1] != ')')
        return;
    path[len - 1] = '\0';

    // open file
    FILE* fp = fopen(path, "r");
    if (fp == NULL)
        return;
    char buf[1024];
    message send_message;
    send_message.status = 0;

    // read database/table/column
    if (!fgets(buf, sizeof(buf), fp))
        return;
    char* col_name = malloc((strlen(buf) + 1) * sizeof(char));
    strcpy(col_name, buf);
    char* db_name = strsep(&col_name, ".");
    char* tbl_name = strsep(&col_name, ".");
    if (col_name == NULL || db_name == NULL || tbl_name == NULL) {
        log_err("-- Load file has improper format.\n");
    }
    log_info("-- Loading: %s/%s/%s\n", db_name, tbl_name, col_name);

    // read all rows from file and send to server
    size_t query_size = 22 + strlen(db_name) + strlen(tbl_name) + 1024;
    char* query_insert = malloc(sizeof(char) * query_size);
    while (fgets(buf, sizeof(buf), fp)) {
        size_t len = strlen(buf);
        if (buf[len - 1] == '\n')
            buf[len - 1] = '\0';
            len--;
        sprintf(query_insert, "relational_insert(%s.%s,%s)", db_name, tbl_name, buf);
        send_message.length = 21 + strlen(db_name) + strlen(tbl_name) + len;
        send_message.payload = query_insert;
        sendMessage(send_message, socket); 
        receiveMessage(socket);
    }
}

void handlePipeQuery(char* query, int socket) {
    // extract message path
    char* path = query + 5;
    path = trim_whitespace(path);
    path = trim_quotes(path);
    size_t len = strlen(path);
    if (path[len - 1] != ')')
        return;
    path[len - 1] = '\0';

    // open file
    FILE* fp = fopen(path, "r");
    if (fp == NULL)
        return;
    char buf[1024];
    char* command = malloc(sizeof(char) * 1025);
    message send_message;
    send_message.status = 0;

    // read database/table/column
    while (fgets(buf, sizeof(buf), fp)) {
        if (strncmp(buf, "load", 4) == 0) {
            handleLoadQuery(buf, socket);
            continue;
        }
        if (strncmp(buf, "--", 2) == 0)
            continue;
        size_t len = strlen(buf);
        if (buf[len - 1] == '\n')
            buf[len - 1] = '\0';
            len--;
        sprintf(command, "%s", buf);
        send_message.length = len;
        send_message.payload = command;
        sendMessage(send_message, socket); 
        receiveMessage(socket);
    }
}

int main(void)
{
    signal(SIGPIPE, SIG_IGN);

    int client_socket = connect_client();
    if (client_socket < 0) {
        exit(1);
    }

    // Always output an interactive marker at the start of each command if the
    // input is from stdin. Do not output if piped in from file or from other fd
    char* prefix = "";
    if (isatty(fileno(stdin))) {
        prefix = "db_client > ";
    }

    // Continuously loop and wait for input. At each iteration:
    // 1. output interactive marker
    // 2. read from stdin until eof.
    char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
    message send_message;
    send_message.payload = read_buffer;
    char *output_str = NULL;

    while (printf("%s", prefix), output_str = fgets(read_buffer,
           DEFAULT_STDIN_BUFFER_SIZE, stdin), !feof(stdin)) {
        if (output_str == NULL) {
            log_err("-- fgets failed.\n");
            break;
        }
        log_info("-- Received client query: %s", read_buffer);

        // handle load messages differently from the rest
        if (strncmp(read_buffer, "load", 4) == 0) {
            handleLoadQuery(read_buffer, client_socket);
            continue;
        }

        // handle pipe messages differently from the rest
        if (strncmp(read_buffer, "pipe", 4) == 0) {
            handlePipeQuery(read_buffer, client_socket);
            continue;
        }

        // check message length and send
        send_message.length = strlen(read_buffer);
        if (send_message.length <= 0)
            continue;
        sendMessage(send_message, client_socket);
        receiveMessage(client_socket);
    }
    close(client_socket);
    return 0;
}
