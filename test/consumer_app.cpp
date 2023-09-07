#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>  // for usleep

#include <string>

#include "currentTime.h"
#include "image.h"
#include "node/node_app.h"

using namespace node;

class ConsumerNode : public NodeApp {
public:
  void Update() override {
    if (last_msg_count == msg_count) {
      printf("Timed out waiting for message.\n");
      received_first_val1 = false;
      received_first_val2 = false;
      fflush(stdout);
      if (++timeout > 10) {
        printf("Didn't see another message; exiting.\n");
        SetState(FINISHED);
      }
    } else {
      timeout = 0;
    }
    last_msg_count = msg_count;
  }

  void Finalize() override {}

  void HandleImage1(MsgPtr<node_msg::image>& message1) {
    printf(" - 1. Got new message %d %p\n", message1->rows, reinterpret_cast<void*>(message1.get()));
    // For demonstration, here's how to retain a pointer to message()
    // after the handler.  But we don't need to do this if we just operate
    // on the message in the handler, and not afterward.
    MsgPtr<node_msg::image>& img1 = message1;
    printf(" - 1. Message Count %d\n", mcount);
    printf(" - 1. Now starting consumer\n");
    fflush(stdout);

    printf(" - 1. Acquiring data (%d)... \n", msg_count);
    fflush(stdout);

    double now = time_now();
    printf(" - 1. Now: %.2f, TimeStamp: %.2f\n", now, img1->timestamp);
    printf(" - 1. Value of rows: %d (expected %d), time diff: %.2f us\n", img1->rows, val1 + 1,
           (now - img1->timestamp) * 1000000.0);
    fflush(stdout);

    if (!received_first_val1) {
      val1 = img1->rows;
      received_first_val1 = true;
    } else {
      // Very simple way to check that we are receiving in order and not missing
      assert(static_cast<int>(img1->rows) == val1 + 1);
      val1 = img1->rows;
    }

    // maybe do something with img here
    usleep(10000);

    fflush(stdout);
    if (++msg_count == mcount) {
      SetState(FINISHED);
    }
    printf(" - 1. done\n");
  }

  void HandleImage2(MsgPtr<node_msg::image>& message2) {
    printf(" - 2. Got new message %d %p\n", message2->rows, reinterpret_cast<void*>(message2.get()));
    // For demonstration, here's how to retain a pointer to message()
    // after the handler.  But we don't need to do this if we just operate
    // on the message in the handler, and not afterward.
    MsgPtr<node_msg::image>& img2 = message2;
    printf(" - 2. Message Count %d\n", mcount);
    printf(" - 2. Now starting consumer\n");
    fflush(stdout);

    printf(" - 2. Acquiring data (%d)... \n", msg_count);
    fflush(stdout);

    double now = time_now();
    printf(" - 2. Now: %.2f, TimeStamp: %.2f\n", now, img2->timestamp);
    printf(" - 2. Value of rows: %d (expected %d), time diff: %.2f us\n", img2->rows, val2 + 1,
           (now - img2->timestamp) * 1000000.0);
    fflush(stdout);

    if (!received_first_val2) {
      val2 = img2->rows;
      received_first_val2 = true;
    } else {
      // Very simple way to check that we are receiving in order and not missing

      assert(static_cast<int>(img2->rows) == val2 + 1);
      val2 = img2->rows;
    }

    // maybe do something with img here
    usleep(10000);

    fflush(stdout);
    if (++msg_count == mcount) {
      SetState(FINISHED);
    }
    printf(" - 2. done\n");
  }

  ConsumerNode(int count)
      : NodeApp("ConsumerNode") {
    mcount = count;
    // const char* topic = "topic";
    // const char* type_msg = "rows";
    Subscribe(&ConsumerNode::HandleImage1, "topic");
    Subscribe(&ConsumerNode::HandleImage2, "topic3");
  }

  // Other state for test application
  int msg_count = 0;
  int val1 = 0;
  int val2 = 0;
  int last_msg_count = -1;
  int timeout = 0;
  bool received_first_val1 = false;
  bool received_first_val2 = false;

  int count2 = 0;

private:
  int mcount = 5000;
};

std::unique_ptr<NodeApp> node::InitializeNodeApp(int argc, char* argv[], const NodeApp::Options& options) {
  (void)options;
  printf("Hello, I am a consumer of messages\n");

  int count = 5000;
  printf("Args count : %d\n", argc);
  for (int i = 1; i < argc; i++) {
    if (!strcmp(argv[i], "-m")) {
      count = atoi(argv[i + 1]);
    }
  }

  return std::unique_ptr<NodeApp>(new ConsumerNode(count));
}
