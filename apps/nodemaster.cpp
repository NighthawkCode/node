#include "node/nodemaster.h"

#include <stdio.h>
#include <vlog.h>

#include "node/nodeerr.h"

int main(int argc, char** argv) {
  char* data_path = nullptr;
  char* peer_list = nullptr;
  uint16_t node_reg_port = DEFAULT_NODE_REGISTRY_PORT;
  if (argc > 1) data_path = argv[1];
  if (argc > 2) peer_list = argv[2];
  if (argc > 3) {
    sscanf(argv[3], "%hu", &node_reg_port);
  }
  int result = node::nodemaster_main(data_path, peer_list, node_reg_port);
  vlog_fine(VCAT_NODE, "EXIT nodemaster_main  data=%s, peers=%s, port=%hu", data_path, peer_list,
            node_reg_port);
  return result;
}
