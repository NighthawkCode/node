#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>  // for usleep

#include "currentTime.h"
#include "image.h"
#include "node/core.h"

static void Usage() {
  printf("Node consumer test\n");
  printf("Usage: cons [OPTIONS]\n");
  printf("  -h: Display this help\n");
  printf("  -m <count>: Specify number of messages to process, default: 50\n");
}

int main(int argc, char** argv) {
  node::core core;
  int count = 350;

  for (int i = 1; i < argc; i++) {
    if (!strcmp(argv[i], "-h")) {
      Usage();
      return 0;
    } else if (!strcmp(argv[i], "-m")) {
      if (i + 1 >= argc) {
        Usage();
        return -1;
      }
      count = atoi(argv[i + 1]);
      i++;
    }
  }

  printf("Hello, I am a consumer of messages\n");

  auto image_channel = core.subscribe<node_msg::var_image>("topic2");
  image_channel->set_node_name("large consumer test");

  node::NodeError res = image_channel->open();
  if (res != node::SUCCESS) {
    u32 t = 0;
    while (res == node::PRODUCER_NOT_PRESENT) {
      // In this case, wait for some time until the producer starts
      // this could be a race condition in some cases
      usleep(2000000);
      t += 2;
      printf(".");
      if (t >= 20) {
        break;
      }
      res = image_channel->open();
    }

    if (res != node::SUCCESS) {
      fprintf(stderr, "Failed to open the topic: %d\n", res);
      return -1;
    }
  }

  unsigned int value = 0;
  bool initial_vals = false;
  int initial = 0;

  printf("Now starting consumer \n");
  fflush(stdout);
  for (int it = 0; it < count; it++) {
    node::MsgPtr<node_msg::var_image> msg = image_channel->get_message(res);
    node_msg::var_image img = *msg;

    printf(" releasing data ... ");
    fflush(stdout);
    image_channel->release_message(msg);
    printf(" RELEASED!\n");

    int timeout = 0;

    while (res != node::SUCCESS) {
      printf("Timed out (%d), waiting\n", timeout);
      fflush(stdout);
      initial_vals = false;
      if (++timeout > 5) {
        // Likely the producer is no longer around
        printf(" No data received in a while, terminating ...\n");
        return 0;
      }
      res = image_channel->open(1.0, 1.0, 0.0);
      if (res == node::SUCCESS) {
        node::MsgPtr<node_msg::var_image> msg2 = image_channel->get_message(res);
        img = *msg2;

        printf(" releasing data ... ");
        fflush(stdout);
        image_channel->release_message(msg2);
        printf(" RELEASED!\n");
      }
      usleep(1000000);
    }

    printf(" - Acquiring data (%d)... ", it);
    fflush(stdout);

    double now = time_now();

    printf("Value of rows: %d (expected %d), time diff: %f us\n", img.val, value + 1,
           (now - img.timestamp) * 1000000.0);
    fflush(stdout);
    if (!initial_vals or initial < 2) {
      value = img.val;
      initial += 1;
      // initial_vals = true;
    } else {
      initial_vals = true;
      // Very simple way to check that we are receiving in order and not missing
      assert(img.val == value + 1);
      value = img.val;
    }
    // maybe do something with img here

    usleep(100000);
  }

  return 0;
}
