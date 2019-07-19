#include <stdio.h>
#include <stdlib.h>
#include <unistd.h> // for usleep
#include "image.h"
#include "node/node_app.h"

using namespace node;

class ProducerNode : public NodeApp {
public:
  ProducerNode() {}

  void Connect(const char *topic) {
    image_channel_ = core_.provides<node_msg::image>(topic);

    node::NodeError res = image_channel_.open();
    if (res == node::SUCCESS) {
      SetState(RUNNING);
    } else {
      fprintf(stderr, "Failure to create a topic (%d), maybe you need to restart the node server\n", res);
      SetState(FAILED);
    }
    usleep(2000000);
  }

  bool HandleNextMessage() override {
    printf(" - Acquiring data (%d)... ", msg_number_);
    fflush(stdout);
    node_msg::image* img = image_channel_.prepare_message();
    printf("Previous value of rows: %d ", img->rows);
    img->rows = msg_number_;
    img->cols = 3+msg_number_;
    img->format = 12;
    img->timestamp = 0;
    for(int i=0; i<256; i++) img->pixels[i] = i*msg_number_;
    
    printf(" publishing data (%d) ... ", msg_number_);
    fflush(stdout);
    image_channel_.transmit_message( img );
    printf(" PUBLISHED!\n");
    usleep(1000000);
    msg_number_++;
    if (msg_number_ == 50) {
      SetState(FINISHED);
    }

    return true;
  }

  int msg_number_ = 0;
  node::publisher<node_msg::image> image_channel_;
};

std::unique_ptr<NodeApp> node::InitializeNodeApp(int argc, char *argv[],
                                                 const NodeApp::Options &options) {
  const char* topic = "topic";
  if( argc == 2 ) {
    topic = argv[1];
  }
  printf("Hello, I am a producer of messages. I will publish topic: %s\n", topic );

  auto *producer = new ProducerNode();
  producer->Connect(topic);
  return std::unique_ptr<NodeApp>(producer);
}
