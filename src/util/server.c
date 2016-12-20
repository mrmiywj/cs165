#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <errno.h>

#include "api/cs165.h"
#include "api/context.h"
#include "api/persist.h"
#include "parse/parse.h"
#include "query/execute.h"
#include "util/const.h"
#include "util/message.h"
#include "util/log.h"
#include "util/debug.h"
#include "api/btree.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
void handle_client(int client_socket) {
    int done = 0;
    int length = 0;
    bool shutdown = false;

    log_info("Connected to socket: %d.\n", client_socket);

    // Create two messages, one from which to read and one from which to receive
    message send_message;
    message recv_message;

    // create the client context here
    ClientContext* new_context = malloc(sizeof(ClientContext));
    new_context->queries = NULL;
    new_context->chandle_table = NULL;
    new_context->chandles_in_use = 0;
    new_context->chandle_slots = 0;
    new_context->client_fd = client_socket;
    insertContext(new_context);

    do {
        // receive query metadata
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        if (length < 0) {
            log_err("-- Client connection closed!\n");
            break;
        } else if (length == 0)
            done = true;

        // while not done
        if (done)
            break;

        // initialize receiving buffer
        char recv_buffer[recv_message.length];
        length = recv(client_socket, recv_buffer, recv_message.length,0);
        recv_message.payload = recv_buffer;
        recv_message.payload[recv_message.length] = '\0';
        recv_message.status = OK_DONE;
        recv_message.length = 0;

        // check for shutdown
        if (strncmp(recv_message.payload, "shutdown", 8) == 0) {
            log_info("-- Shutting down!\n");
            shutdown = true;
            break;
        }

        log_info("-- Received query from client: %s\n", recv_message.payload);

        // parse command for content
        send_message.status = OK_DONE;
        send_message.length = 0;
        send_message.payload = NULL;
        DbOperator* query = parse_command(recv_message.payload, &send_message, client_socket, new_context);

        // handle query and execute
        char* result = executeDbOperator(query, &send_message);
        send_message.length = strlen(result);
        // print server response to send during every query
        char* copy = malloc((strlen(result) + 1) * sizeof(char));
        strcpy(copy, result);
        char* ptr = copy;
        while (*ptr != '\0') {
            if (*ptr == '\n')
                *ptr = ' ';
            ptr++;
        }
        log_info("-- Server response: \"%s\", length %i, status %i\n", copy, send_message.length, send_message.status);

        // send status and meta of response message
        if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
            log_err("Failed to send message metadata, error %i.\n", errno);
            writeDb();
            exit(1);
        }

        // send message payload if necessary
        if (send_message.status == OK_WAIT_FOR_RESPONSE && (int) send_message.length > 0) {
            if (send(client_socket, result, send_message.length, 0) == -1) {
                log_err("Failed to send message payload, error %i.\n", errno);
                writeDb();
                exit(1);
            }
        }
        
        // print context every call
        printContext(new_context);

        log_info("==============================================================");
        log_info("==================== DONE WITH THIS QUERY ====================");
        log_info("==============================================================\n");
    } while (!done);

    log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);
    // delete context and write db to file
    deleteContext(new_context);
    writeDb();
    if (shutdown == true)
        exit(0);
}

int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    // look for an open socket
    log_info("Attempting to setup server...\n");
    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    // socket meta
    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    /*
    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }
    */

    // bind (lock) the current socket
    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    // listen on the current socket
    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}

int main(void) {
    // BTreeUNode* node = createBTreeU();
    // for (int i = 500; i > 0; i--) {
    //     insertValueU(&node, i, 0);
    //     printTreeU(node, "");
    // }
    // for (int i = 500; i > 0; i--) {
    //     deleteValueU(&node, i, i - 1);
    //     printTreeU(node, "");
    // }
    // exit(0);

    signal(SIGPIPE, SIG_IGN);

    // set up socket
    int server_socket = setup_server();
    if (server_socket < 0)
        exit(1);

    // load database files
    startupDb();

    // wait for a connection
    log_info("==============================================================");
    log_info("================= Waiting for client at %d.. =================", server_socket);
    log_info("==============================================================\n");

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket = 0;

    // block until we get a connection
    while ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) != -1) {
        // handle incoming connection
        handle_client(client_socket);
    }

    return 0;
}

