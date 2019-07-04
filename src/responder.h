#pragma once 

namespace node {

struct responder
{
    responder() {
        start_node_responder();
    }
    ~responder() {
        stop_node_responder();
    }

    void start_node_responder();
    void stop_node_responder();
};

}  // namespace node

