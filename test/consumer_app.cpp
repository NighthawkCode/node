#include <stdio.h>
#include <stdlib.h>
#include <unistd.h> // for usleep
#include <assert.h>
#include "image.h"
#include "node/node_app.h"

using namespace node;

class ConsumerNode : public NodeApp {
public:
  ConsumerNode() {}

  void Connect() {
    image_channel_ = core_.subscribe<node_msg::image>("topic");

    auto res = image_channel_.open(30);
    SetState(res == NodeError::SUCCESS ? RUNNING : FAILED);
  }

  bool HandleNextMessage() override {
    NodeError res;
  
    printf("Now starting consumer [%d] \n", image_channel_.get_index()); fflush(stdout);

    printf(" - Acquiring data (%d)... ", msg_count);
    fflush(stdout);
    node_msg::image *img = image_channel_.get_message(res);
    if (res != SUCCESS) {
      // Likely the producer is no longer around
      printf(" No data received in a while, terminating ...\n");
      SetState(NodeApp::FINISHED);
      return false;
    }

    printf("Value of rows: %d (expected %d) ", img->rows, val+1);
    fflush(stdout);

    if (!received_first_val) {
      val = img->rows;
      received_first_val = true;
    } else {
      // Very simple way to check that we are receiving in order and not missing
      assert(img->rows == val +1);
      val = img->rows;
    }
    
    // maybe do something with img here
    usleep(1000000);
    
    printf(" releasing data ... ");
    fflush(stdout);
    image_channel_.release_message( img );
    printf(" RELEASED!\n");
    if (++msg_count == 50) {
      SetState(FINISHED);
    }
    return true;
  }

  // Node-related fields
  node::subscriber<node_msg::image> image_channel_;

  // Other state for test application
  int msg_count = 0;
  int val = 0;
  bool received_first_val = false;
};

std::unique_ptr<NodeApp> node::InitializeNodeApp(int argc, char *argv[],
                                                 const NodeApp::Options &options) {
  printf("Hello, I am a consumer of messages\n");
  auto *consumer = new ConsumerNode;
  consumer->Connect();
  return std::unique_ptr<NodeApp>(consumer);
}

