#ifndef ODB_TYPES_H
#define ODB_TYPES_H

#include <Python.h>

typedef struct  {
    PyObject_HEAD
    int handle;
    int npools;
    int ntables;
} ODBConnection;

static  PyTypeObject ODBConnectionType;

#endif
