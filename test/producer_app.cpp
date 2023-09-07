#include <stdio.h>
#include <unistd.h>  // for usleep

#include "currentTime.h"
#include "image.h"
#include "node/node_app.h"

using namespace node;

class ProducerNode : public NodeApp {
public:
  ProducerNode(const std::string topic)
      : NodeApp("ProducerNode") {
    ProvidesSHM<node_msg::image>(topic, 100);
    usleep(2000000);
  }

  void Update() override {
    printf(" - Acquiring data (%d)... ", msg_number_);
    fflush(stdout);
    auto img = PrepareMessage<node_msg::image>();

    img->rows = msg_number_;
    img->cols = 3 + msg_number_;
    img->format = 12;
    img->timestamp = 0;
    for (int i = 0; i < 256; i++) img->pixels[i] = i * msg_number_;

    printf(" publishing data (%d) ... ", msg_number_);
    fflush(stdout);
    img->timestamp = time_now();
    TransmitMessage(img);
    printf(" PUBLISHED!\n");
    usleep(100000);
    msg_number_++;
    if (msg_number_ == 250) {
      SetState(FINISHED);
    }
  }

  void Finalize() override {}

  int msg_number_ = 1;
  node::publisher<node_msg::image> image_channel_;
};

std::unique_ptr<NodeApp> node::InitializeNodeApp(int argc, char* argv[], const NodeApp::Options& options) {
  (void)options;
  const char* topic = "topic3";
  if (argc == 2) {
    topic = argv[1];
  }
  printf("Hello, I am a producer of messages. I will publish topic: %s\n", topic);

  return std::unique_ptr<NodeApp>(new ProducerNode(topic));
}
