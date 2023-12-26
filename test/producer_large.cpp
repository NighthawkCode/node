#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>  // for usleep

#include "currentTime.h"
#include "image.h"
#include "node/core.h"

static void Usage() {
  printf("Node producer test\n");
  printf("Usage: prod [OPTIONS]\n");
  printf("  -h: Display this help\n");
  printf("  -n: Publish a network topic (default is SHM)\n");
  printf("  -t <topic_name>: Publish on a given topic (default: 'topic')\n");
  printf("  -f <frequency>: Specify a publishing frequency, default: 10Hz\n");
  printf("  -m <count>: Specify number of messages to publish, default: 50\n");
  printf("  -s <image_size>: Specify a large size of image, default: 100000\n");
}

int main(int argc, char** argv) {
  node::core core;

  bool publish_network = true;
  int publish_frequency = 10;
  const char* topic = "topic2";
  int count = 50;
  int img_size = 50000;

  for (int i = 1; i < argc; i++) {
    if (!strcmp(argv[i], "-h")) {
      Usage();
      return 0;
    } else if (!strcmp(argv[i], "-n")) {
      publish_network = true;
    } else if (!strcmp(argv[i], "-t")) {
      if (i + 1 >= argc) {
        Usage();
        return -1;
      }
      topic = argv[i + 1];
      i++;
    } else if (!strcmp(argv[i], "-f")) {
      if (i + 1 >= argc) {
        Usage();
        return -1;
      }
      publish_frequency = atoi(argv[i + 1]);
      i++;
    } else if (!strcmp(argv[i], "-m")) {
      if (i + 1 >= argc) {
        Usage();
        return -1;
      }
      count = atoi(argv[i + 1]);
      i++;
    } else if (!strcmp(argv[i], "-s")) {
      if (i + 1 >= argc) {
        Usage();
        return -1;
      }
      img_size = atoi(argv[i + 1]);
      i++;
    }
  }

  printf("Hello, I am a producer of messages. I will publish topic: %s\n", topic);

  auto image_channel = core.provides<node_msg::var_image>(topic);

  // List existing topics

  node::NodeError res = image_channel.open(100, publish_network);
  if (res != node::SUCCESS) {
    // TODO: make it so the node server does not need restart
    fprintf(stderr, "Failure to create a topic (%d), maybe you need to restart the node server\n", res);
    return -1;
  }

  printf("Topic number %d\n", res);
  unsigned int microsecond_sleep = 1000000 / publish_frequency;
  // Wait a bit so consumers can attach if needed
  usleep(2000000);
  printf("Now starting publishing\n");

  for (int it = 0; it < count; it++) {
    printf(" - Acquiring data (%d)... ", it);
    fflush(stdout);

    node_msg::var_image* img = image_channel.prepare_message();

    printf("Previous value: %d ", img->val);
    img->val = it;
    img->data.resize(img_size);
    for (int i = 0; i < img_size; i++) img->data[i] = i * it;

    printf(" publishing data (%d) ... ", it);
    fflush(stdout);
    img->timestamp = time_now();
    image_channel.transmit_message(img);
    printf(" PUBLISHED!\n");
    usleep(microsecond_sleep);
  }
  return 0;
}
