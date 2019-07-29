#include <stdio.h>
#include <stdlib.h>
#include <unistd.h> // for usleep
#include "image.h"
#include "node/node_app.h"

using namespace node;

class ProducerNode : public NodeApp {
public:
  ProducerNode(const std::string topic) {
    Provides<node_msg::image>(topic);
    usleep(2000000);
  }

  void Update() override {
    printf(" - Acquiring data (%d)... ", msg_number_);
    fflush(stdout);
    auto img = PrepareMessage<node_msg::image>();
    printf("Previous value of rows: %d ", img->rows);
    img->rows = msg_number_;
    img->cols = 3+msg_number_;
    img->format = 12;
    img->timestamp = 0;
    for(int i=0; i<256; i++) img->pixels[i] = i*msg_number_;
    
    printf(" publishing data (%d) ... ", msg_number_);
    fflush(stdout);
    TransmitMessage(img);
    printf(" PUBLISHED!\n");
    usleep(1000000);
    msg_number_++;
    if (msg_number_ == 50) {
      SetState(FINISHED);
    }
  }

  int msg_number_ = 1;
  node::publisher<node_msg::image> image_channel_;
};

std::unique_ptr<NodeApp> node::InitializeNodeApp(int argc, char *argv[],
                                                 const NodeApp::Options &options) {
  const char* topic = "topic";
  if( argc == 2 ) {
    topic = argv[1];
  }
  printf("Hello, I am a producer of messages. I will publish topic: %s\n", topic );

  return std::unique_ptr<NodeApp>(new ProducerNode(topic));
}
