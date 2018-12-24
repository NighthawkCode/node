#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <fcntl.h>
#include <errno.h>
#include "nodecore.h"

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

bool nodecore::open()
{
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

    return true;
}

// Get the number of open channels on the system
u32 nodecore::num_channels()
{
    return 0;
}

// This function retrieves the channel info based on the index, the 
// info parameter is output. The function returns false if there is no 
// channel on that index
bool nodecore::get_topic_info(u32 channel_index, topic_info& info)
{
    return false;
}

// Create a new channel on the system, with the information on info
bool nodecore::create_topic(const topic_info& info)
{
    return false;
}

// Get information on a topic on the system, based on the name of the topic.
// returns false if there is no topic with that name
bool nodecore::get_topic_info(const std::string& name, topic_info& info)
{
    return false;
}

void* helper_open_channel(const channel_info& info)
{
    return nullptr;
}

nodecore::~nodecore()
{
    if (addr != nullptr ) {
        munmap(addr, mem_length);
        addr = nullptr;
    }
    if (mem_fd != 0) {
        close(mem_fd);
        mem_fd = 0;
    }
}

