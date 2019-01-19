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
#include "registry.h"
#include "image.h"

#define NODE_REGISTRY_PORT 25678
#define SERVER_IP          "127.0.0.1"

#define BUFFER_SIZE        8196

int main(int argc, char *argv[])
{
    int sockfd = 0, n = 0;
    char *sendBuffer = new char[BUFFER_SIZE];
    char *recvBuffer = new char[BUFFER_SIZE];
    struct sockaddr_in serv_addr; 

    memset(recvBuffer, 0, BUFFER_SIZE);
    if((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf("\n Error : Could not create socket \n");
        return 1;
    } 

    memset(&serv_addr, 0, sizeof(serv_addr)); 

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(NODE_REGISTRY_PORT); 

    if(inet_pton(AF_INET, SERVER_IP, &serv_addr.sin_addr)<=0) {
        printf("\n inet_pton error occured\n");
        return 1;
    } 

    if( connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
       printf("\n Error : Connect Failed \n");
       return 1;
    } 

    // First, send our request to the server
    node_msg::registry_request request;
    request.action = node_msg::CREATE_TOPIC;
    request.topic_name = "my_topic";
    request.msg_hash = node_msg::image::TYPE_HASH;
    request.msg_name = node_msg::image::TYPE_STRING;
    request.chn_path = "/my_topic";
    request.chn_size = 10;

    size_t enc_size = request.encode_size();
    bool ret = request.encode(sendBuffer, BUFFER_SIZE);
    if (!ret) {
        printf("Error encoding\n");
        exit(-1);
    }
    n = write(sockfd, sendBuffer, enc_size);
    if (n != enc_size) {
        printf("Error sending request, wanted to send %ld bytes but only sent %d bytes\n", enc_size, n);
        exit(-1);
    }

    node_msg::registry_reply reply;
    n = read(sockfd, recvBuffer, BUFFER_SIZE-1);
    if (n <= 0) {
        printf("Error receiving a reply\n");
        exit(-1);
    }
    ret = reply.decode(recvBuffer, n);
    if (!ret) {
        printf("Error decoding the received reply\n");
        exit(-1);
    }

    printf("Reply from the server:\n");
    printf("Return status: %d\n", reply.status);
    printf("Topic name: %s\n", reply.topic_name.c_str());
    printf("Msg name: %s\n", reply.msg_name.c_str());
    printf("Msg Hash: %08lX\n", reply.msg_hash);
    printf("Chn Path: %s\n", reply.chn_path.c_str());
    printf("Chn size: %d\n", reply.chn_size);

    close(sockfd);
    delete sendBuffer;
    delete recvBuffer;
    return 0;
}