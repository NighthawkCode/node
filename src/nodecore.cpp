#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <fcntl.h>
#include <errno.h>

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

int main_int(int argc, char **argv)
{
    const int mem_length = 128 * KB;

    {
        node_access node_lock;

        int ret = node_lock.access("/tmp/node.lock", true);
        if (!ret) {
            exit(-1);
        }

        int mem_fd = shm_open("/verdant_node", O_RDWR | O_CREAT, S_IRWXU);
        printf("Hello this is a Test\n");
        printf("Got fd: %d\n", mem_fd);

        // Ensure we have enough space
        ftruncate(mem_fd, mem_length);

        void *addr = mmap(NULL, mem_length, PROT_READ | PROT_WRITE,
                          MAP_SHARED, mem_fd, 0);

        if (addr == (void *)-1) {
            printf("Got error as: %d\n", errno);
            munmap(addr, mem_length);
            close(mem_fd);
            return -1;
        }

        printf("Got address: %p\n", addr);
        printf("First integer is %X\n", *(unsigned int *)addr);

        unsigned int *ui = (unsigned int *)addr;
        *ui = 3;
        printf("After write, first integer is: %X\n", *ui);

        munmap(addr, mem_length);
        close(mem_fd);
    }

    return 0;
}

