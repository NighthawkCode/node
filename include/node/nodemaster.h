#pragma once
#include <cstdint>

#include "nodedefault.h"

namespace node {

extern int nodemaster_main(const char* data_path, const char* peer_list, uint16_t node_reg_port);

extern void nodemaster_main_quit();
extern void nodemaster_quit_callback(void (*cb)(void));
extern void nodemaster_shutdown_callback(void (*cb)(void));

}  // namespace node
