// Main file of pynode module.
#include <arpa/inet.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

#include "CBufParserPy.h"
#include "node/nodelib.h"
#include "node/transport.h"
#include "pycbuf.h"
#include "pynode.h"
#include "vlog.h"

// clang-format off
typedef struct {
  PyObject_HEAD
  node::observer* obs;
  CBufParserPy* parser;
} pynodesub;
// clang-format on

// --------------------------------------------------------------------------

static bool _convert_iter(PyObject* obj, PyObject** it) {
  PyObject* tmp = PyObject_GetIter(obj);
  if (tmp == NULL) return false;

  *it = tmp;
  return true;
}

static bool _convert_optional_size(PyObject* obj, Py_ssize_t* s) {
  if (obj == Py_None) return true;

  PyObject* n = PyNumber_Index(obj);
  if (n == NULL) return false;

  Py_ssize_t tmp = PyLong_AsSsize_t(n);
  Py_DECREF(n);

  if (PyErr_Occurred()) return false;

  *s = tmp;
  return true;
}

static bool _is_valid_ip_string(const char* ip) {
  struct sockaddr_in sa;
  int result = inet_pton(AF_INET, ip, &(sa.sin_addr));
  return result != 0;
}

// pynodesub --------------------------------------------------------------------------

PyDoc_STRVAR(pynode_pynodesub_close___doc__,
             "close(self)\n"
             "--\n"
             "\n"
             "Free all memory and related informaton regarding the subscriber.\n");

static PyObject* pynode_pynodesub_close_impl(pynodesub* self) {
  if (self->obs) {
    self->obs->preclose();
    delete self->obs;
    self->obs = nullptr;
    delete self->parser;
    self->parser = nullptr;
  }
  Py_RETURN_NONE;
}

// --------------------------------------------------------------------------

static PyObject* pynode_pynodesub___new__(PyTypeObject* type, PyObject* args, PyObject* kwds) {
  pynodesub* self;

  assert(type != NULL && type->tp_alloc != NULL);
  self = (pynodesub*)type->tp_alloc(type, 0);

  return (PyObject*)self;
}

// --------------------------------------------------------------------------

PyDoc_STRVAR(pynode_pynodesub___init____doc__,
             "\n"
             "Create a PyNodeSub to access messages on a topic. Parameters:"
             "topic_name: String with the name of the topic to subscribe\n"
             "\n");

static inline int pynode_pynodesub___init___impl(pynodesub* self, const char* topic_name) {
  /* Allow calling __init__ more than once, in that case make sure to
     release any previous object reference */
  if (self->obs != nullptr) {
    delete self->obs;
  }

  self->obs = new node::observer();

  self->obs->set_topic_name(topic_name);
  self->obs->set_node_name("pynode");
  // TODO: make the timeouts optional inputs
  node::NodeError err = self->obs->open(0.01f, 0.05f);

  if (err != node::SUCCESS) {
    std::string strerr =
        "Could not open topic " + std::string(topic_name) + " . Error Message: " + node::NodeErrorToStr(err);
    PyErr_SetString(PyExc_Exception, strerr.c_str());
    return -1;
  }

  self->parser = new CBufParserPy();
  if (!self->parser->ParseMetadata(self->obs->msg_cbuftxt(), self->obs->msg_type())) {
    std::string strerr = "Could not parse message information for topic " + std::string(topic_name);
    PyErr_SetString(PyExc_Exception, strerr.c_str());
    return -1;
  }
  return 0;
}

static int pynode_pynodesub___init__(PyObject* self, PyObject* args, PyObject* kwargs) {
  char* topic_name = nullptr;
  if (!PyArg_ParseTuple(args, "s:PyNodeSub_init", &topic_name)) return -1;
  return pynode_pynodesub___init___impl((pynodesub*)self, topic_name);
}

// --------------------------------------------------------------------------

PyDoc_STRVAR(pynode_pynodesub_is_there_new___doc__,
             "\n"
             "Returns if there is a new message on to be read\n"
             "\n");

static PyObject* pynode_pynodesub_is_there_new_impl(PyObject* self) {
  pynodesub* sub = (pynodesub*)self;
  if (sub->obs == nullptr) {
    PyErr_SetString(PyExc_Exception, "The subscriber was not correctly setup");
    return nullptr;
  }
  return PyBool_FromLong(sub->obs->is_there_new());
}

// --------------------------------------------------------------------------

PyDoc_STRVAR(pynode_pynodesub_num_published___doc__,
             "\n"
             "Returns the number of published messages so far (not the read ones)\n"
             "\n");

static PyObject* pynode_pynodesub_num_published_impl(PyObject* self) {
  pynodesub* sub = (pynodesub*)self;
  if (sub->obs == nullptr) {
    PyErr_SetString(PyExc_Exception, "The subscriber was not correctly setup");
    return nullptr;
  }
  return PyLong_FromLong(sub->obs->get_num_published());
}

// --------------------------------------------------------------------------

PyDoc_STRVAR(pynode_pynodesub_get_message___doc__,
             "\n"
             "Returns a message from the topic.\n"
             "Possible behaviors: \n");

static PyObject* pynode_pynodesub_get_message_impl(PyObject* self, PyObject* args, PyObject* kwargs) {
  pynodesub* sub = (pynodesub*)self;
  if (sub->obs == nullptr) {
    PyErr_SetString(PyExc_Exception, "The subscriber was not correctly setup");
    return nullptr;
  }
  node::NodeError err;
  // Hardcoding timeout...
  node::MsgBufPtr msg = sub->obs->get_message(err, 0.1f);
  if (err != node::SUCCESS) {
    std::string strerr = "Error on obtaining message: ";
    strerr += node::NodeErrorToStr(err);
    PyErr_SetString(PyExc_Exception, strerr.c_str());
    return nullptr;
  }

  // Python can be slow, do a copy for now. This could be an option
  std::vector<u8> vec;
  vec.resize(msg.getSize());
  memcpy(vec.data(), msg.getMem(), msg.getSize());
  sub->obs->release_message(msg);

  PyObject* pynodemod = pynode_getmodule();
  PyNode_State* state = pynodemodule_getstate(pynodemod);
  PyObject* pymsg = nullptr;
  sub->parser->FillPyObject(sub->obs->msg_hash(), sub->obs->msg_type().c_str(), vec.data(), vec.size(), "sub",
                            state->pycbuf_module, pymsg);

  // No setting exception here since FillPyObject should already have done it
  // if we got a nullptr
  return pymsg;
}

// --------------------------------------------------------------------------

static const char* BoolToStr(bool b) { return b ? "YES" : "NO"; }

static PyObject* pynode_pynodesub___repr___impl(pynodesub* self) {
  std::string return_value;
  return_value = std::string("Topic_name: ") + self->obs->topic_name().data();
  return_value += " Msg type: " + self->obs->msg_type();
  return_value += std::string(" Unread messages: ") + BoolToStr(self->obs->is_there_new());
  return_value += " Num Published: " + std::to_string(self->obs->get_num_published());

  return PyUnicode_FromFormat(return_value.c_str());
}

// ------------------------------------------------------------------------

static void pynodesub_dealloc(pynodesub* self) {
  if (self->obs) {
    delete self->obs;
    self->obs = nullptr;
    delete self->parser;
    self->parser = nullptr;
  }
  Py_TYPE(self)->tp_free(self);
}

static int pynodesub_clear(pynodesub* self) {
  // Py_CLEAR(self->source);
  return 0;
}

static int pynodesub_traverse(pynodesub* self, visitproc visit, void* arg) {
  // Py_VISIT( some PyObject )
  return 0;
}

// clang-format off
static struct PyMemberDef pynodesub_members[] = {
    {NULL, 0, 0, 0, NULL}  /* Sentinel */
};

static struct PyMethodDef pynodesub_methods[] = {
    {"close",         (PyCFunction)  pynode_pynodesub_close_impl,         METH_NOARGS,                   pynode_pynodesub_close___doc__},
    {"get_message",   (PyCFunction)  pynode_pynodesub_get_message_impl,   METH_VARARGS | METH_KEYWORDS,  pynode_pynodesub_get_message___doc__},
    {"is_there_new",  (PyCFunction)  pynode_pynodesub_is_there_new_impl,  METH_NOARGS,                   pynode_pynodesub_is_there_new___doc__},
    {"num_published", (PyCFunction)  pynode_pynodesub_num_published_impl, METH_NOARGS,                   pynode_pynodesub_num_published___doc__},
    {NULL, NULL, 0, NULL}  /* sentinel */
};

static PyTypeObject PyNodeSub_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name      = "pynode.PyNodeSub",
    .tp_basicsize = sizeof(pynodesub),
    .tp_dealloc   = (destructor) pynodesub_dealloc,
    .tp_repr      = (reprfunc) pynode_pynodesub___repr___impl,
    .tp_flags     = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_doc       = pynode_pynodesub___init____doc__,
    .tp_traverse  = (traverseproc) pynodesub_traverse,
    .tp_clear     = (inquiry) pynodesub_clear,
    .tp_methods   = pynodesub_methods,
    .tp_members   = pynodesub_members,
    .tp_init      = pynode_pynodesub___init__,
    .tp_new       = pynode_pynodesub___new__,
};


// --- pynode module ---------------------------------------------------------

PyNode_State* pynodemodule_getstate(PyObject* module) {
  void* state = PyModule_GetState(module);
  assert(state != NULL);
  return (PyNode_State*)state;
}

static int pynodemodule_traverse(PyObject* mod, visitproc visit, void* arg) {
  PyNode_State* state = pynodemodule_getstate(mod);
  if (!state->initialized) return 0;
  Py_VISIT(state->unsupported_operation);
  return 0;
}

static int pynodemodule_clear(PyObject* mod) {
  PyNode_State* state = pynodemodule_getstate(mod);
  if (!state->initialized) return 0;
  Py_CLEAR(state->unsupported_operation);
  return 0;
}

static void pynodemodule_free(PyObject* mod) { pynodemodule_clear(mod); }

PyDoc_STRVAR(pynode_module_get_all_topics___doc__,
             "get_all_topics(ip)\n"
             "--\n"
             "Get all the advertised topcis on that nodemaster\n"
             "the return is a list of the topics (each one a python class)\n"
             "");

static PyObject* pynode_module_get_all_topics_impl(PyObject* module, PyObject* args) {
  char* ip = nullptr;
  if (!PyArg_ParseTuple(args, "s", &ip)) {
    return nullptr;
  }

  // Check that the ip is a valid format
  if (!_is_valid_ip_string(ip)) {
    PyErr_SetString(PyExc_Exception,
                    "Function get_all_topics requires an argument to be an ip like 192.168.1.1");
    return nullptr;
  }

  std::vector<node::topic_info> topics;
  auto err = node::get_all_topic_info(topics, ip);
  if (err != node::SUCCESS) {
    std::string error_str = "Error obtaining list of topics: ";
    error_str += node::NodeErrorToStr(err);
    PyErr_SetString(PyExc_Exception, error_str.c_str());
    return nullptr;
  }

  // Convert the vector of topics into a python list
  PyNode_State* state = pynodemodule_getstate(module);
  auto& parser = *state->topic_info_parser;
  // 4096 scratch buffer
  std::vector<u8> bytebuffer;
  bytebuffer.resize(4096);
  PyObject* list = PyList_New(topics.size());
  for (size_t idx = 0; idx < topics.size(); idx++) {
    auto& topic = topics[idx];
    auto topic_sz = topic.encode_net_size();
    if (topic_sz > bytebuffer.size()) {
      bytebuffer.resize(topic_sz);
    }
    if (!topic.encode_net((char*)bytebuffer.data(), bytebuffer.size())) {
      std::string err_str =
          "Error encoding topic index " + std::to_string(idx) + " with name " + topic.topic_name;
      PyErr_SetString(PyExc_Exception, err_str.c_str());
      return nullptr;
    }
    PyObject* pytopic = nullptr;
    parser.FillPyObject(node::topic_info::TYPE_HASH, node::topic_info::TYPE_STRING, bytebuffer.data(),
                        topic_sz, "pynode_get_all_topics", state->pycbuf_module, pytopic);
    if (pytopic == nullptr) {
      // No setting exception here since FillPyObject should already have done it
      return nullptr;
    }
    PyList_SetItem(list, idx, pytopic);
  }

  return list;
}

PyDoc_STRVAR(pynode_module_num_topics___doc__,
             "num_topics(ip)\n"
             "--\n"
             "Get the number of advertised topics on that nodemaster\n"
             "");

static PyObject* pynode_module_num_topics_impl(PyObject* self, PyObject* args) {
  PyObject* return_value = nullptr;
  char* ip = nullptr;
  if (!PyArg_ParseTuple(args, "s", &ip)) {
    return return_value;
  }

  // Check that the ip is a valid format
  if (!_is_valid_ip_string(ip)) {
    PyErr_SetString(PyExc_Exception, "Function num_topics requires an argument to be an ip like 192.168.1.1");
    return return_value;
  }

  u32 num;
  auto err = node::num_channels(num, ip);
  if (err != node::SUCCESS) {
    std::string error_str = "Error obtaining number of topics: ";
    error_str += node::NodeErrorToStr(err);
    PyErr_SetString(PyExc_Exception, error_str.c_str());
    return return_value;
  }
  return_value = PyLong_FromLong(num);
  return return_value;
}

// clang-format off
static struct PyMethodDef pynodemodule_methods[] = {
    {"num_topics", (PyCFunction)pynode_module_num_topics_impl, METH_VARARGS,  pynode_module_num_topics___doc__},
    {"get_all_topics", (PyCFunction)pynode_module_get_all_topics_impl, METH_VARARGS,  pynode_module_get_all_topics___doc__},
    {NULL, NULL, 0, NULL} /* sentinel */
};
// clang-format on

struct PyModuleDef PyNode_Module = {
    PyModuleDef_HEAD_INIT,
    .m_name = "pynode",
    .m_doc = NULL,
    .m_size = sizeof(PyNode_State),
    .m_methods = pynodemodule_methods,
    .m_traverse = pynodemodule_traverse,
    .m_clear = pynodemodule_clear,
    .m_free = (freefunc)pynodemodule_free,
};

PyObject* pynode_getmodule(void) { return PyState_FindModule(&PyNode_Module); }

PyMODINIT_FUNC PyInit_pynode(void) {
  PyObject* m = NULL;
  PyObject* _io = NULL;
  PyNode_State* state = NULL;
  bool bret = false;

  /* Create the module and initialize state */
  m = PyModule_Create(&PyNode_Module);
  if (m == NULL) goto exit;
  state = pynodemodule_getstate(m);
  state->initialized = false;
  state->unsupported_operation = NULL;

  if (PyType_Ready(&PyNodeSub_Type) < 0) goto fail;
  if (PyModule_AddObject(m, "PyNodeSub", (PyObject*)&PyNodeSub_Type) < 0) goto fail;

  /* Import the _io module and get the `UnsupportedOperation` exception */
  _io = PyImport_ImportModule("_io");
  if (_io == NULL) goto fail;
  state->unsupported_operation = PyObject_GetAttrString(_io, "UnsupportedOperation");
  if (state->unsupported_operation == NULL) goto fail;
  if (PyModule_AddObject(m, "UnsupportedOperation", state->unsupported_operation) < 0) goto fail;

  state->pycbuf_module = PyImport_ImportModule("pycbuf");
  if (state->pycbuf_module == nullptr) goto fail;

  /* Increate the reference count for classes stored in the module */
  Py_INCREF(state->unsupported_operation);

  state->topic_info_parser = new CBufParserPy();
  bret =
      state->topic_info_parser->ParseMetadata(node::topic_info::cbuf_string, node::topic_info::TYPE_STRING);
  if (!bret) goto fail;

  state->initialized = true;
exit:
  return m;

fail:
  Py_DECREF(m);
  Py_XDECREF(state->unsupported_operation);
  if (state->topic_info_parser != nullptr) delete state->topic_info_parser;
  state->topic_info_parser = nullptr;
  return NULL;
}
