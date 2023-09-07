#include <stdio.h>
#include <stdlib.h>

#include "image.h"
#include "node/core.h"

double time_now() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  double now = double(ts.tv_sec) + double(ts.tv_nsec) / 1e9;

  return now;
}

int main() {
  node::core core;

  printf("Hello, I am a consumer of messages, using notify\n");

  auto image_channel = core.subscribe<node_msg::image>("topic");
  image_channel->set_node_name("consumer_notify");
  node::NotificationManager nm;

  unsigned int val = 0;
  bool first_val = false;

  node::NodeError res = image_channel->open_notify(nm, [&](node::MsgPtr<node_msg::image>& img) {
    double now = time_now();
    printf("Value of rows: %d (expected %d), time diff: %f us\n", img->rows, val + 1,
           (now - img->timestamp) * 1000000.0);
    fflush(stdout);
    if (!first_val) {
      val = img->rows;
      first_val = true;
    } else {
      // Very simple way to check that we are receiving in order and not missing
      //             assert(img->rows == val +1);
      val = img->rows;
    }
  });

  if (res != node::SUCCESS) {
    fprintf(stderr, "Failed to open the topic: %d\n", res);
    return -1;
  }

  printf("Now starting consumer \n");
  fflush(stdout);
  for (int it = 0; it < 500; it++) {
    printf(" - Acquiring data (%d)... ", it);
    fflush(stdout);
    nm.wait_for_notification();
  }
  return 0;
}
