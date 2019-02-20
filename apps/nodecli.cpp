#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h> 
#include <inttypes.h>
#include "defer.h"
#include "registry.h"
#include "node/nodeerr.h"

#define NODE_REGISTRY_PORT 25678
#define SERVER_IP          "127.0.0.1"

#define BUFFER_SIZE        8196

using namespace node;

void Usage()
{
    printf("Node command line toolset, v1.0\n");
    printf("  Usage: node [OPTIONS] COMMAND\n");
    printf("  Options:\n");
    printf("    --ip <server ip>   : use the designated IP for the node server\n");
    printf("    -v                 : verbose\n");
    printf("    -h                 : show this help\n");
    printf("\n");
    printf("  Commands:\n");
    printf("    ls   : list all topics on the system\n");
    exit(0);
}

static NodeError send_request(const std::string &server_ip, 
                         node_msg::registry_request &request, 
                         node_msg::registry_reply &reply)
{
    int sockfd = 0, n = 0;
    char *sendBuffer = new char[BUFFER_SIZE];
    char *recvBuffer = new char[BUFFER_SIZE];
    struct sockaddr_in serv_addr; 

    DEFER( close(sockfd) );
    DEFER( delete sendBuffer );
    DEFER( delete recvBuffer );

    memset(recvBuffer, 0, BUFFER_SIZE);
    if((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        fprintf(stderr, "\n Error : Could not create socket \n");
        return SOCKET_COULD_NOT_BE_CREATED;
    } 

    memset(&serv_addr, 0, sizeof(serv_addr)); 

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(NODE_REGISTRY_PORT); 

    if(inet_pton(AF_INET, server_ip.c_str(), &serv_addr.sin_addr)<=0) {
        fprintf(stderr, "Seerver ip: %s\n", server_ip.c_str());
        fprintf(stderr, "inet_pton error occured\n");
        return SOCKET_SERVER_IP_INCORRECT;
    }

    if( connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        fprintf(stderr, "Node registry server could not be found at %s:%d\n",
            server_ip.c_str(), NODE_REGISTRY_PORT);
        return SOCKET_SERVER_NOT_FOUND;
    }

    size_t enc_size = request.encode_size();
    bool ret = request.encode(sendBuffer, BUFFER_SIZE);
    if (!ret) {
        fprintf(stderr, "Error encoding\n");
        return IDL_ENCODE_ERROR;
    }
    n = write(sockfd, sendBuffer, enc_size);
    if (n != (ssize_t)enc_size) {
        fprintf(stderr, "Error sending request, wanted to send %ld bytes but only sent %d bytes\n", enc_size, n);
        return SOCKET_WRITE_ERROR;
    }

    n = read(sockfd, recvBuffer, BUFFER_SIZE-1);
    if (n <= 0) {
        fprintf(stderr, "Error receiving a reply\n");
        return SOCKET_REPLY_ERROR;
    }
    ret = reply.decode(recvBuffer, n);
    if (!ret) {
        fprintf(stderr, "Error decoding the received reply\n");
        return IDL_DECODE_ERROR;
    }

    return SUCCESS;
}


enum Command
{
    UNKNOWN, 
    LIST_TOPICS
};

bool DoListTopics(std::string &server_ip)
{
    node_msg::registry_request req = {};
    node_msg::registry_reply reply = {};
    NodeError res;

    req.action = node_msg::NUM_TOPICS;
    req.cli = 123;
    res = send_request(server_ip, req, reply);
    if (res != SUCCESS) {
        printf("Error on communication: %d\n", res);
        return false;
    }

    unsigned int num_topics = reply.num_topics;
    printf("Found %d topics on the system\n\n", num_topics);
    for(unsigned int i=0; i<num_topics; i++) {
        req = {};
        reply = {};

        req.action = node_msg::TOPIC_AT_INDEX;
        req.cli = 123;
        res = send_request(server_ip, req, reply);
        if (res != SUCCESS) {
            printf("Error on communication: %d\n", res);
            return false;
        }
        printf(" TOPIC:    %s\n", reply.topic_name.c_str());
        printf(" MSG NAME: %s\n", reply.msg_name.c_str());
        printf(" MSG HASH: %" PRIx64 "\n", reply.msg_hash);
        printf(" CHN PATH: %s\n", reply.chn_path.c_str());
        printf(" CHN SIZE: %d\n", reply.chn_size);
        printf(" PUB PID:  %d\n", reply.publisher_pid);
        printf(" VISIBLE:  %d\n", reply.visible);
        printf("\n");
    }

    return true;
}

int main(int argc, char *argv[])
{
    std::string server_ip = SERVER_IP;
    bool verbose = false;
    Command cmd = UNKNOWN;

    if (argc < 2) Usage();

    int i = 1;
    while(i < argc) {
        if (!strcmp(argv[i], "--ip")) {
            if (i+1 == argc) Usage();
            server_ip = argv[i+1];
            i++;
        } else if (!strcmp(argv[i], "-v")) {
            verbose = true;
        } else if (!strcmp(argv[i], "-h")) {
            Usage();
        } else if (!strcmp(argv[i], "ls")) {
            if (cmd != UNKNOWN) {
                printf("Two commands cannot be used at once\n");
                exit(-1);
            }
            cmd = LIST_TOPICS;
        } else {
            printf("Unrecognized option: %s\n", argv[i]);
        }
        i++;
    }

    switch(cmd) {
        case LIST_TOPICS:
            DoListTopics(server_ip);
        break;
        default:
            printf("\n Error: unknown command\n");
    }

    return 0;
}