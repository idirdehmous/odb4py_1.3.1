#define PY_SSIZE_T_CLEAN
#define NPY_NO_DEPRECATED_API NPY_2_0_API_VERSION
#include <Python.h>
#include "odb_types.h"
#include "io_module.c"
#include "dict_module.h"
#include "dbarray_module.c"
#include "dca_module.c"
#include "geo_module.c"



// little doc 
PyDoc_STRVAR(open_doc      , "A C/python wrapper of the original ODBc_open method. Establish a connection to the ODB and initalize its paths and structures");
PyDoc_STRVAR(close_doc     , "Closes a connection to an opened ODB");
PyDoc_STRVAR(npar_doc      , "Fetch ODB rows as a numpy array with optional header containing column names." );
PyDoc_STRVAR(dict_doc      , "Fetch ODB rows as a python  dictionnary where the column names are the keys and the values are lists of values.");
PyDoc_STRVAR(dcaf_doc      , "Create DCA files (Direct Column  Access ).");
PyDoc_STRVAR(geop_doc      , "Returns lat,lon ,pressure , altitude , date , time from ODB. Has the option  'sql_cond' to add additional SQL statement");
PyDoc_STRVAR(dist_doc      , "Compute great circle distance between numpy lat/lon pairs. Optimized with Numpy C/API.");

PyDoc_STRVAR(core_doc,"C/Python interface to access the ODB1 IFS/ARPEGE databases\nThe original source code has been developed by S.Saarinen et al\n***Copyright (c) 1997-98, 2000 ECMWF. All Rights Reserved !***");



static PyMethodDef ODBConnection_methods[] = {
    {"odb_open"  , (PyCFunction) (void(*)(void)) odb_open_method, METH_VARARGS | METH_KEYWORDS, open_doc},
    {"odb_close" , (PyCFunction) odb_close_method, METH_NOARGS, close_doc},
    {"odb_dict"  , (PyCFunction) (void(*)(void))odb_dict_method,  METH_VARARGS | METH_KEYWORDS, dict_doc},
    {"odb_array" , (PyCFunction) (void(*)(void))odb_array_method,  METH_VARARGS | METH_KEYWORDS,npar_doc},
    {"odb_dca"   , (PyCFunction)(void(*)(void)) odb_dca_method, METH_VARARGS | METH_KEYWORDS,  dcaf_doc },
    {"odb_geopoints", (PyCFunction)(void(*)(void)) odb_geopoints_method, METH_VARARGS | METH_KEYWORDS, geop_doc },
    {"odb_gcdist",(PyCFunction)(void(*)(void)) odb_gcdist_method, METH_VARARGS | METH_KEYWORDS,  dist_doc },
    {NULL, NULL, 0, NULL}
};



// It ensure that ODB is closed even the odb_close function is not called !
static void ODBConnection_dealloc(ODBConnection *self)
{
    if (self->handle > 0) {
        ODBc_close(self->handle);
        Py_TYPE(self)->tp_free((PyObject *)self);
    }
}


// Init the Type 
static  PyTypeObject ODBConnectionType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "odb.ODBConnection",       // Name
    .tp_basicsize = sizeof(ODBConnection),  // size 
    .tp_flags = Py_TPFLAGS_DEFAULT,         // other flags
    .tp_methods = ODBConnection_methods,     // ODBConnection  register 
    .tp_dealloc = (destructor)ODBConnection_dealloc    // destroys  ODBConnection
};





// Define the module itself 
static struct PyModuleDef   odb_core = {
    PyModuleDef_HEAD_INIT,
    "core"         ,
    core_doc      , 
    -1             ,
    ODBConnection_methods ,
    .m_slots =NULL
};



// Create the IO module 
PyMODINIT_FUNC PyInit_core  (void) {

if (PyType_Ready(&ODBConnectionType) < 0) {
    return NULL;
}
    PyObject*  m  ;
    PyObject* ModuleError ;
    m=PyModule_Create(&odb_core);
    if ( m == NULL) {
        ModuleError = PyErr_NewException("Failed to create the module : odb4py.core", NULL, NULL);
        Py_XINCREF(ModuleError) ;
        return NULL;
}

// Add a reference  of the new Type 
Py_INCREF(&ODBConnectionType);
PyModule_AddObject(m, "ODBConnection", (PyObject *)&ODBConnectionType);
return m  ;
}

