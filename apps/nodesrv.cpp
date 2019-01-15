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

#define NODE_REGISTRY_PORT 25678
#define NUM_CONNECTIONS    10
#define BUFFER_SIZE        8196

int main(int argc, char **argv)
{
    int server_fd, valread; 
    struct sockaddr_in address; 
    int opt = 1; 
    int addrlen = sizeof(address); 
    char *buffer = new char[BUFFER_SIZE]; 

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
    printf("Now starting to listen on the socket...\n");

    while(1) {
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
        node_msg::registry_request request;
        bool ret = request.decode(buffer, valread);
        if (!ret) {
            printf("Error decoding the request\n");
            close(new_socket);
            continue;
        }

        printf("Request to the server:\n");
        printf("Create topic: %d\n", request.create_topic);
        printf("Topic name: %s\n", request.topic_name.c_str());
        printf("Msg name: %s\n", request.msg_name.c_str());
        printf("Msg Hash: %08lX\n", request.msg_hash);
        printf("Chn Path: %s\n", request.chn_path.c_str());
        printf("Chn size: %d\n", request.chn_size);

        // TODO: Here we would do internal work for the request, creation, query...

        node_msg::registry_reply reply;
        reply.return_val = 0; // everything's fine
        reply.topic_name = request.topic_name;
        reply.msg_name = request.msg_name;
        reply.msg_hash = request.msg_hash;
        reply.chn_path = request.chn_path;
        reply.chn_size = request.chn_size;

        size_t reply_size = reply.encode_size();
        ret = reply.encode(buffer, BUFFER_SIZE);
        if (!ret) {
            printf("Error encoding the reply\n");
            close(new_socket);
            continue;            
        }

        send(new_socket , buffer , reply_size , 0 ); 

        close(new_socket);
    }

    return 0;
}