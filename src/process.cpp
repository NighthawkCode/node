#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include "process.h"

u32 get_my_pid()
{
    return (u32)getpid();
}

bool is_pid_alive(u32 pid)
{
    char buf[1024];
    snprintf(buf, sizeof(buf), "/proc/%d", pid);
    
    struct stat stat_buf;
    auto ret = stat(buf, &stat_buf);
    if (ret == 0) return true;
    return false;
}