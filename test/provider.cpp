#include <stdio.h>
#include <unistd.h>  // for usleep

#include "node/core.h"
#include "rpcmsg.h"
#include "vlog.h"

int main() {
  node::core core;

  printf("Hello, I am a provider of messages\n");

  auto rpc_srv = core.service_requests<rpc_msg_test::request, rpc_msg_test::reply>("rpctopic");

  rpc_srv.setCallback([](rpc_msg_test::request* request, rpc_msg_test::reply* reply) {
    reply->c = request->a + request->b;
    return true;
  });

  node::NodeError res = rpc_srv.open();
  if (res != node::SUCCESS) {
    printf("Failed to create the topic: %d\n", res);
  }

  printf("Now starting to service requests \n");
  fflush(stdout);
  double start_test_time = time_now();
  while (time_now() - start_test_time < 6.0) {
    sleep(1);
  }
  printf("Overall we serviced %zu queries \n", rpc_srv.get_num_requests_handled());
  return 0;
}
