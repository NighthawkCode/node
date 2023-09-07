#pragma once

/*
 * Core header of Node
 *
 */

#include <string>

#include "node/transport.h"

// NVTX instrumentation macros
#ifdef HAVE_CUDA
#include <nvToolsExt.h>
#define NVTX_NAME_THREAD(tid, name) nvtxNameOsThread(tid, name)
#define NVTX_RANGE_PUSH(name) nvtxRangePush(name)
#define NVTX_RANGE_POP() nvtxRangePop()
#define NVTX_MARK(name) nvtxMark(name)
#else
#define NVTX_NAME_THREAD(tid, name) (void)0
#define NVTX_RANGE_PUSH(name) (void)0
#define NVTX_RANGE_POP() (void)0
#define NVTX_MARK(name) void(0)
#endif

namespace node {

class core {
public:
  template <typename T>
  publisher<T> provides(const std::string& topic_name) {
    publisher<T> pub;
    pub.set_topic_name(topic_name);
    return pub;
  }

  template <typename T>
  std::unique_ptr<subscriber<T>> subscribe(const std::string& topic_name) {
    std::unique_ptr<subscriber<T>> sub = std::make_unique<subscriber<T>>();
    sub->set_topic_name(topic_name);
    return sub;
  }

  observer observe(const std::string& topic_name) {
    observer obs;
    obs.set_topic_name(topic_name);
    return obs;
  }

  template <class RequestT, class ReplyT>
  response_provider<RequestT, ReplyT> service_requests(const std::string& topic_name) {
    response_provider<RequestT, ReplyT> prov;
    prov.set_topic_name(topic_name);
    return prov;
  }

  template <class RequestT, class ReplyT>
  requester<RequestT, ReplyT> issue_requests(const std::string& topic_name) {
    requester<RequestT, ReplyT> req;
    req.set_topic_name(topic_name);
    return req;
  }

  //        void wait_for_next_message_in_subscribers();
};

const char* NodeErrorToStr(node::NodeError err);

}  // namespace node
