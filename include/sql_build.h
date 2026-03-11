#ifndef SQL_BUILDER_H
#define SQL_BUILDER_H

// Concat sql parts for geo_module.c  

typedef struct {
    PyObject *parts;   // python SQL parts 
} SQLBuilder;


static int
is_blank_string(const char *s)
{
    if (!s) return 1;

    while (*s) {
        if (!isspace((unsigned char)*s))
            return 0;   //  find a character 
        s++;
    }
    return 1;           // spaces only 
}

static SQLBuilder *
sqlbuilder_new(void)
{
    SQLBuilder *b = PyMem_Malloc(sizeof(SQLBuilder));
    if (!b) return NULL;
    b->parts = PyList_New(0);
    if (!b->parts) {
        PyMem_Free(b);
        return NULL;
    }
    return b;
}	


static int sqlbuilder_add(SQLBuilder *b, const char *text)
{
    PyObject *s = PyUnicode_FromString(text);
    if (!s) return -1;
    int rc = PyList_Append(b->parts, s);
    Py_DECREF(s);
    return rc;
}

static int
sqlbuilder_addf(SQLBuilder *b, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);

    va_list args_copy;
    va_copy(args_copy, args);

    int needed = vsnprintf(NULL, 0, fmt, args_copy);
    va_end(args_copy);

    if (needed < 0) {
        va_end(args);
        return -1;
    }

    char *buffer = PyMem_Malloc(needed + 1);
    if (!buffer) {
        va_end(args);
        return -1;
    }

    vsnprintf(buffer, needed + 1, fmt, args);
    va_end(args);

    PyObject *s = PyUnicode_FromString(buffer);
    PyMem_Free(buffer);

    if (!s)
        return -1;
    int rc = PyList_Append(b->parts, s);
    Py_DECREF(s);
    return rc;
}



static PyObject *sqlbuilder_build(SQLBuilder *b)
{
    PyObject *empty = PyUnicode_FromString("");
    if (!empty) return NULL;
    PyObject *sql = PyUnicode_Join(empty, b->parts);
    Py_DECREF(empty);
    return sql;
}


static void sqlbuilder_free(SQLBuilder *b){
    if (!b) return;
    Py_XDECREF(b->parts);
    PyMem_Free(b);
}


#endif
