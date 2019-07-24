#include <unistd.h> 
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h> 
#include <stdlib.h> 
#include <netinet/in.h>
#include <arpa/inet.h> 
#include <string.h> 
#include <errno.h>
#include "registry.h"
#include "node_registry.h"
#include "responder.h"

#define NODE_REGISTRY_PORT 25678
#define NUM_CONNECTIONS    10
#define BUFFER_SIZE        8196

#define DEBUG 0

void printRequest(const node_msg::registry_request& request);
void printReply(const node_msg::registry_reply& reply);

namespace node {

static bool nodecore_quit = false;

void nodecore_main_quit() 
{
    nodecore_quit = true;
}

static void TopicInfoToReply(const node::topic_info &inf, node_msg::registry_reply& reply)
{
    reply.topic_name = inf.name;
    reply.msg_name = inf.message_name;
    reply.msg_hash = inf.message_hash;
    reply.chn_path = inf.cn_info.channel_path;
    reply.chn_size = inf.cn_info.channel_size;
    reply.publisher_pid = inf.publisher_pid;
    reply.visible  = inf.visible;
}

int nodecore_main()
{
    int server_fd = -1;
    int valread; 
    struct sockaddr_in address; 
    int opt = 1; 
    int addrlen = sizeof(address); 
    char *buffer = new char[BUFFER_SIZE]; 

    node::node_registry reg;
    printf("Starting server\n");

    // Creating socket file descriptor 
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) { 
        perror("socket failed"); 
        exit(EXIT_FAILURE); 
    } 
       
    // Forcefully attaching socket to the port 
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, 
                                                  &opt, sizeof(opt))) 
    { 
        perror("setsockopt"); 
        exit(EXIT_FAILURE); 
    } 
    address.sin_family = AF_INET; 
    address.sin_addr.s_addr = INADDR_ANY; 
    address.sin_port = htons( NODE_REGISTRY_PORT ); 
       
    // Forcefully attaching socket to the port 8080 
    if (bind(server_fd, (struct sockaddr *)&address,  
        sizeof(address))<0) { 
        perror("bind failed"); 
        exit(EXIT_FAILURE); 
    } 

    if (listen(server_fd, NUM_CONNECTIONS) < 0) { 
        perror("listen"); 
        exit(EXIT_FAILURE); 
    } 
    
    node::responder respond;

    printf("Now starting to listen on the socket...\n");

    while(!nodecore_quit) {
        int new_socket;
        memset(buffer, 0, 8196);
        new_socket = accept(server_fd, (struct sockaddr *)&address,  
                    (socklen_t*)&addrlen);

        if (new_socket < 0) {
            perror("accept"); 
            exit(EXIT_FAILURE); 
        }

        //inform user of socket number - used in send and receive commands  
        printf("New connection , socket fd is %d , ip is : %s , port : %d\n", 
                new_socket , inet_ntoa(address.sin_addr) , ntohs(address.sin_port));

        // Handle here the request
        valread = read( new_socket , buffer, BUFFER_SIZE); 
        node_msg::registry_request request = {};
        node_msg::registry_reply reply = {};

        bool ret = request.decode(buffer, valread);
        if (!ret) {
            printf("Error decoding the request\n");
            close(new_socket);
            continue;
        }

        printRequest(request);

        // TODO: Here we would do internal work for the request, creation, query...
        if (request.action == node_msg::CREATE_TOPIC) {
            node::topic_info inf = {};
            inf.name = request.topic_name;
            inf.message_name = request.msg_name;
            inf.message_hash = request.msg_hash;
            inf.cn_info.channel_path = request.chn_path;
            inf.cn_info.channel_size = request.chn_size;
            inf.publisher_pid = request.publisher_pid;
            ret = reg.create_topic(inf);
            if (!ret) {
                printf(">> Create topic failed\n");
                reply.status = node_msg::GENERIC_ERROR; // error
            } else {
                printf(">> Create topic succeeded\n");
                reply.status = node_msg::SUCCESS;
                reply.topic_name = request.topic_name;
                reply.msg_name = request.msg_name;
                reply.msg_hash = request.msg_hash;
                reply.chn_path = request.chn_path;
                reply.chn_size = request.chn_size;
            }
        } else if (request.action == node_msg::ADVERTISE_TOPIC) {
            node::topic_info inf = {};
            inf.name = request.topic_name;
            // Maybe ensure the pid of the request matches what is stored
            ret = reg.make_topic_visible(inf);
            if (ret) reply.status = node_msg::SUCCESS;
            else reply.status = node_msg::TOPIC_NOT_FOUND;
        } else if (request.action == node_msg::TOPIC_BY_NAME) {
            // search topic by name 
            node::topic_info inf = {};
            ret = reg.get_topic_info(request.topic_name, inf);
            if (!ret) {
                printf(">> Query Topic by name failed\n");
                reply.status = node_msg::TOPIC_NOT_FOUND;
            } else {
                printf(">> Query Topic by name succeeded\n");
                reply.status = node_msg::SUCCESS;
                TopicInfoToReply(inf, reply);
            }
        } else if (request.action == node_msg::NUM_TOPICS) {
            reply.status = node_msg::SUCCESS;
            if (request.cli == 123) {
                reply.num_topics = reg.num_topics_cli();
            } else {
                reply.num_topics = reg.num_topics();
            }
            printf(">> Query Num Topics returned %d topics\n", reply.num_topics);
        } else if (request.action == node_msg::TOPIC_AT_INDEX) {
            node::topic_info inf = {};
            if (request.cli == 123) {
                ret = reg.get_topic_info_cli(request.topic_index, inf);
            } else { 
                ret = reg.get_topic_info(request.topic_index, inf);
            }   
            if (!ret) {
                printf(">> Query Topic by index failed\n");
                reply.status = node_msg::INDEX_OUT_OF_BOUNDS;
            } else {
                printf(">> Query Topic by index succeeded\n");
                reply.status = node_msg::SUCCESS;
                TopicInfoToReply(inf, reply);
            }
        } else {
            reply.status = node_msg::REQUEST_INVALID;
        }

        size_t reply_size = reply.encode_size();
        ret = reply.encode(buffer, BUFFER_SIZE);
        if (!ret) {
            printf("Error encoding the reply\n");
            close(new_socket);
            continue;            
        }

#if DEBUG
        printReply(reply);
#endif
        send(new_socket , buffer , reply_size , 0 ); 

        close(new_socket);
    }

    if( server_fd != -1 ) {
        close(server_fd);
    }
    return 0;
}

}  // namespace node
