#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <fcntl.h>
#include <errno.h>
#include <semaphore.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <string.h>
#include <arpa/inet.h> 

#include "defer.h"
#include "nodelib.h"
#include "registry.h"
#include "process.h"
#include "circular_buffer.h"

/*
    In order for this to work, it is preferable if we disable ASLR
    This way, the address from mmap will be consistent across processes

    https://askubuntu.com/questions/318315/how-can-i-temporarily-disable-aslr-address-space-layout-randomization

    sudo sysctl kernel.randomize_va_space=0
*/

#define KB (1024)
#define MB (KB * KB)

class node_access
{
  public:
    int lock_fd;

    node_access()
    {
        lock_fd = -1;
    }

    bool access(const char *lock_name, bool exclusive)
    {
        lock_fd = open(lock_name, O_RDWR | O_CREAT, 0744);
        if (lock_fd == -1)
        {
            perror("Could not open the lock file: ");
            return false;
        }

        int mode = (exclusive? LOCK_EX : LOCK_SH);

        int lock_return = flock(lock_fd, mode);
        if (lock_return == -1)
        {
            perror("Could not get the lock for node: ");
            return false;
        }
        return true;
    }

    ~node_access()
    {
        if (lock_fd != -1) {
            flock(lock_fd, LOCK_UN);
            close(lock_fd);
            lock_fd = -1;
        }
    }
};

static const int mem_length = 128 * KB;
// Put this in a header?
#define BUFFER_SIZE 4096
#define NODE_REGISTRY_PORT 25678

#define DEBUG 1

static const char *RequestTypeToStr[] = {
    "CREATE_TOPIC",
    "ADVERTISE_TOPIC",
    "NUM_TOPICS",
    "TOPIC_AT_INDEX",
    "TOPIC_BY_NAME"
};

static const char *RequestStatusToStr[] = {
    "SUCCESS",
    "TOPIC_NOT_FOUND",
    "INDEX_OUT_OF_BOUNDS",
    "REQUEST_INVALID",
    "GENERIC_ERROR"
};

void printRequest(const node_msg::registry_request& request)
{
    printf("Request with data:\n");
    printf("  RequestType: %s\n", RequestTypeToStr[request.action]);
    printf("  Topic name: %s\n", request.topic_name.c_str());
    printf("  Msg name: %s\n", request.msg_name.c_str());
    printf("  Msg Hash: %08lX\n", request.msg_hash);
    printf("  Chn Path: %s\n", request.chn_path.c_str());
    printf("  Chn size: %d\n", request.chn_size);
}

void printReply(const node_msg::registry_reply& reply)
{
    printf("  Return status: %s\n", RequestStatusToStr[reply.status]);
    printf("  Topic name: %s\n", reply.topic_name.c_str());
    printf("  Msg name: %s\n", reply.msg_name.c_str());
    printf("  Msg Hash: %08lX\n", reply.msg_hash);
    printf("  Chn Path: %s\n", reply.chn_path.c_str());
    printf("  Chn size: %d\n", reply.chn_size);
}

static bool send_request(const std::string &server_ip, 
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

#if DEBUG
    printRequest(request);
#endif

    memset(recvBuffer, 0, BUFFER_SIZE);
    if((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        fprintf(stderr, "\n Error : Could not create socket \n");
        return false;
    } 

    memset(&serv_addr, 0, sizeof(serv_addr)); 

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(NODE_REGISTRY_PORT); 

    if(inet_pton(AF_INET, server_ip.c_str(), &serv_addr.sin_addr)<=0) {
        fprintf(stderr, "Seerver ip: %s\n", server_ip.c_str());
        fprintf(stderr, "inet_pton error occured\n");
        return false;
    }

    if( connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        fprintf(stderr, "Node registry server could not be found at %s:%d\n",
            server_ip.c_str(), NODE_REGISTRY_PORT);
        return false;
    }

    size_t enc_size = request.encode_size();
    bool ret = request.encode(sendBuffer, BUFFER_SIZE);
    if (!ret) {
        fprintf(stderr, "Error encoding\n");
        return false;
    }
    n = write(sockfd, sendBuffer, enc_size);
    if (n != (ssize_t)enc_size) {
        fprintf(stderr, "Error sending request, wanted to send %ld bytes but only sent %d bytes\n", enc_size, n);
        return false;
    }

    n = read(sockfd, recvBuffer, BUFFER_SIZE-1);
    if (n <= 0) {
        fprintf(stderr, "Error receiving a reply\n");
        return false;
    }
    ret = reply.decode(recvBuffer, n);
    if (!ret) {
        fprintf(stderr, "Error decoding the received reply\n");
        return false;
    }

    return true;
}

bool nodelib::open(const std::string& host)
{
   /* 
    node_access node_lock;

    int ret = node_lock.access("/tmp/node.lock", true);
    if (!ret) {
        return false;
    }

    mem_fd = shm_open("/verdant_node", O_RDWR | O_CREAT, S_IRWXU);

    // Ensure we have enough space
    ftruncate(mem_fd, mem_length);

    addr = mmap(NULL, mem_length, PROT_READ | PROT_WRITE,
                        MAP_SHARED, mem_fd, 0);

    if (addr == (void *)-1) {
        printf("Got error as: %d\n", errno);
        addr = nullptr;
        return false;
    }
*/
    if (host.empty()) {
        hostname = "127.0.0.1";
    } else {
        hostname = host;
    }
    node_msg::registry_request req = {};
    node_msg::registry_reply reply = {};

    req.action = node_msg::NUM_TOPICS;
    bool ret = send_request(hostname, req, reply);    
    return ret;
}

// Get the number of open channels on the system
u32 nodelib::num_channels()
{
    node_msg::registry_request req = {};
    node_msg::registry_reply reply = {};

    req.action = node_msg::NUM_TOPICS;
    bool ret = send_request(hostname, req, reply);    
    if (!ret) {
        return 0;
    }
    return reply.num_topics;
}

// This function retrieves the channel info based on the index, the 
// info parameter is output. The function returns false if there is no 
// channel on that index
bool nodelib::get_topic_info(u32 channel_index, topic_info& info)
{
    node_msg::registry_request req = {};
    node_msg::registry_reply reply = {};

    req.action = node_msg::TOPIC_AT_INDEX;
    req.topic_index = channel_index;    
    bool ret = send_request(hostname, req, reply);    
    if (!ret) return false;

    if (reply.status != node_msg::SUCCESS) {
        return false;
    }

    info.name = reply.topic_name;
    info.message_name = reply.msg_name;
    info.message_hash = reply.msg_hash;
    info.cn_info.channel_path = reply.chn_path;
    info.cn_info.channel_size = reply.chn_size;
    return true;
}

bool nodelib::make_topic_visible(const std::string& name)
{
    node_msg::registry_request req = {};
    node_msg::registry_reply reply = {};

    req.action = node_msg::ADVERTISE_TOPIC;
    req.topic_name = name;   
    bool ret = send_request(hostname, req, reply);    
    if (!ret) return false;

    if (reply.status != node_msg::SUCCESS) {
        return false;
    }

    return true;
}

// Create a new channel on the system, with the information on info
bool nodelib::create_topic(const topic_info& info)
{
    node_msg::registry_request req = {};
    node_msg::registry_reply reply = {};

    req.action = node_msg::CREATE_TOPIC;

    req.topic_name = info.name;
    req.msg_hash = info.message_hash;
    req.msg_name = info.message_name;
    req.chn_path = info.cn_info.channel_path;
    req.chn_size = info.cn_info.channel_size;
    req.publisher_pid = get_my_pid();

    bool ret = send_request(hostname, req, reply);    
    if (!ret) {
        // Communication error
        printf("Communication error on send_request\n");
        return false;
    }
    if (reply.status == node_msg::SUCCESS) {
        printf("Create topic succeeded\n");
        return true;
    } else {
        printf("Create topic failed\n");
        return false;
    }
}

// Get information on a topic on the system, based on the name of the topic.
// returns false if there is no topic with that name
bool nodelib::get_topic_info(const std::string& name, topic_info& info)
{
    node_msg::registry_request req = {};
    node_msg::registry_reply reply = {};

    req.action = node_msg::TOPIC_BY_NAME;
    req.topic_name = name;

    bool ret = send_request(hostname, req, reply);    
    if (!ret) return false;

    if (reply.status != node_msg::SUCCESS) {
        return false;
    }

    info.name = reply.topic_name;
    info.message_name = reply.msg_name;
    info.message_hash = reply.msg_hash;
    info.cn_info.channel_path = reply.chn_path;
    info.cn_info.channel_size = reply.chn_size;
    return true;
}

/// This function is meant to create the shared memory for a shm_channel
//  It returns a pointer to the mapped memory for sharing
void* helper_open_channel(const channel_info& info, int& mem_fd)
{
    mem_fd = shm_open(info.channel_path.c_str(), O_RDWR | O_CREAT, S_IRWXU);

    // Ensure we have enough space
    ftruncate(mem_fd, info.channel_size);

    void *addr = mmap(NULL, info.channel_size, PROT_READ | PROT_WRITE,
                        MAP_SHARED, mem_fd, 0);

    if (addr == (void *)-1) {
        printf("Got error as: %d\n", errno);
        addr = nullptr;
        close(mem_fd);
    }

    return addr;
}

void helper_clean(void *addr, int mem_fd, u32 mem_length)
{
    if (addr != nullptr ) {
        munmap(addr, mem_length);
    }
    if (mem_fd != 0) {
        close(mem_fd);
    }
}

