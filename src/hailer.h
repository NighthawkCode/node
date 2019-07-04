#pragma once 

namespace node {

struct hailer
{
    hailer() {
        if( !is_node_running() ) {
            run_node();
        }
        constructed = true; }
    ~hailer() { 
        exit_node();
        constructed = false; }
    bool is_constructed() const {
        return constructed;
    }

private:
    bool is_node_running() const;
    void run_node() const;
    void exit_node() const;

    bool constructed = false;
};

const hailer& get_hailer();

}  // namespace node
