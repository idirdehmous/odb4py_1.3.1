/*
 * odb4py
 * Copyright (C) 2026 Royal Meteorological Institute of Belgium (RMI)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdio.h>

// Version 
#define ODB4PY_VERSION "1.1.3"

// return version  
static PyObject* odbMeta_version(PyObject *self, PyObject *Py_UNUSED(ignored)) {
    return Py_BuildValue("s", ODB4PY_VERSION);
}


// Get platform , build etc  info 
static PyObject* odbMeta_info(PyObject *self, PyObject *Py_UNUSED(ignored)) {
    return Py_BuildValue(
        "{s:s, s:s, s:s, s:s}",
        "version"     , ODB4PY_VERSION,
        "compiler"    , Py_GetCompiler() ,
        "platform"    , Py_GetPlatform() ,
        "python_build", Py_GetBuildInfo()
    );
}


// 
/*static PyObject* odbMeta_env(PyObject *self, PyObject *Py_UNUSED(ignored)) {
    //  set env variable for ODB 
    //  setenv("ODB_IO_METHOD", "default", 0);
    // 
    return Py_BuildValue("s", "ODB environment stub initialized.");
}*/


