#define PY_SSIZE_T_CLEAN
#define NPY_NO_DEPRECATED_API NPY_2_0_API_VERSION
#include <Python.h>
#include "info_module.c"

PyDoc_STRVAR(info_tab_doc  , "Returns all the existing ODB tables.  '386' tables.");
PyDoc_STRVAR(info_var_doc  , "Returns all ODB 'varno' parameters and their descriptions");
PyDoc_STRVAR(info_fun_doc  , "Returns all the possible functions that could be used in ODB sql statement.");
PyDoc_STRVAR(info_doc      ,"A set of C function to get and display the info concerning: the ODB tables ,varno+description  and functions ");



static PyMethodDef info_methods[] = {
    {"odb_tables",(PyCFunction)(void(*)(void))    odb_tables_method, METH_VARARGS | METH_KEYWORDS, info_tab_doc },
    {"odb_varno" ,(PyCFunction)(void(*)(void))    odb_varno_method , METH_VARARGS | METH_KEYWORDS, info_var_doc },
    {"odb_func"  ,(PyCFunction)(void(*)(void))    odb_func_method , METH_VARARGS | METH_KEYWORDS, info_fun_doc },
    {NULL, NULL, 0, NULL}
};


// Define the module itself 
static struct PyModuleDef  odb_info = {
    PyModuleDef_HEAD_INIT,
    "info"         ,
    info_doc      , 
    -1             ,
    info_methods ,
    .m_slots =NULL
};



// Create the IO module 
PyMODINIT_FUNC PyInit_info  (void) {
    
    PyObject*  m  ;
    PyObject* ModuleError ;
    m=PyModule_Create(&odb_info);
    if ( m == NULL) {
        ModuleError = PyErr_NewException("Failed to create the module : odb4py.info", NULL, NULL);
        Py_XINCREF(ModuleError) ;
        return NULL;
}
return m  ;
}

