#include <stdio.h>
#include <stdlib.h>
#include <unistd.h> // for usleep
#include <assert.h>
#include "image.h"
#include "node/node_app.h"

using namespace node;

class ConsumerNode : public NodeApp {
public:
  void Update() override {
    img.release();
    if (last_msg_count == msg_count) {
      printf("Timed out waiting for message.\n");
      fflush(stdout);
      if (++timeout > 5) {
        printf("Didn't see another message; exiting.\n");
        SetState(FINISHED);
      }
    } else {
      timeout = 0;
    }
    last_msg_count = msg_count;
  }
    
  
  void HandleImage(MsgPtr<node_msg::image> &message) {
    printf("Got new message %d %p\n", message->rows, message.get());
    // For demonstration, here's how to retain a pointer to message()
    // after the handler.  But we don't need to do this if we just operate
    // on the message in the handler, and not afterward.
    img = std::move(message);

    printf("Now starting consumer\n");
    fflush(stdout);

    printf(" - Acquiring data (%d)... ", msg_count);
    fflush(stdout);

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
    usleep(10000);
    
    fflush(stdout);
    if (++msg_count == 500) {
      SetState(FINISHED);
    }
    printf("done\n");
  }

  ConsumerNode() {
    Subscribe(&ConsumerNode::HandleImage, "topic");
  }

  // Other state for test application
  int msg_count = 0;
  int val = 0;
  int last_msg_count = -1;
  int timeout = 0;
  bool received_first_val = false;
  MsgPtr<node_msg::image> img;
};

std::unique_ptr<NodeApp> node::InitializeNodeApp(int argc, char *argv[],
                                                 const NodeApp::Options &options) {
  printf("Hello, I am a consumer of messages\n");
  return std::unique_ptr<NodeApp>(new ConsumerNode);
}

