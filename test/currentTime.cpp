#include "currentTime.h"

double time_now() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  double now = double(ts.tv_sec) + (double(ts.tv_nsec) / 1e9);

  return now;
}
