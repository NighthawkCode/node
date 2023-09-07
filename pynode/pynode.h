#pragma once

#include <Python.h>
#include <inttypes.h>

#define VCAT_PYNODE "PYNODE"
class CBufParserPy;

struct PyNode_State {
  int initialized = false;
  PyObject* unsupported_operation = nullptr;
  CBufParserPy* topic_info_parser = nullptr;
  PyObject* pycbuf_module = nullptr;
};

PyNode_State* pynodemodule_getstate(PyObject* module);
PyObject* pynode_getmodule(void);
