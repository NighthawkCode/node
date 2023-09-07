#include <stdio.h>
#include <unistd.h>  // for usleep

#include "node/core.h"
#include "rpcmsg.h"
#include "vlog.h"

int main() {
  node::core core;

  printf("Hello, I am a requester of messages\n");

  auto rpc_req = core.issue_requests<rpc_msg_test::request, rpc_msg_test::reply>("rpctopic");

  node::NodeError res = rpc_req.open();
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
      res = rpc_req.open();
    }

    if (res != node::SUCCESS) {
      fprintf(stderr, "Failed to open the topic: %d\n", res);
      return -1;
    }
  }

  printf("Now starting requests \n");
  fflush(stdout);
  double start_test_time = time_now();
  double sum_request_time = 0;
  u32 num_queries = 0;
  while (time_now() - start_test_time < 3.0) {
    rpc_msg_test::request request;
    rpc_msg_test::reply reply;
    request.a = 3;
    request.b = 10;

    double start_req_time = time_now();
    node::NodeError res = rpc_req.makeRequest(request, reply);
    sum_request_time += time_now() - start_req_time;
    if (res != node::NodeError::SUCCESS) {
      printf("Error on request: %d\n", res);
    }

    if (reply.c != 13.0) {
      printf("The reply is not correct, we got %f\n", reply.c);
    }
    num_queries++;
  }
  double end_time = time_now();
  printf("Overall we did %u queries in %f seconds, each one took on average %.3f microseconds\n", num_queries,
         end_time - start_test_time, sum_request_time * 1E6 / double(num_queries));
  return 0;
}
