#include "process.h"

#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

u32 get_my_pid() { return (u32)getpid(); }

bool is_pid_alive(u32 pid) {
  char buf[1024];
  snprintf(buf, sizeof(buf), "/proc/%d", pid);

  struct stat stat_buf;
  auto ret = stat(buf, &stat_buf);
  if (ret == 0) return true;
  return false;
}
