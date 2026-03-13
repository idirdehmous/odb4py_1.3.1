#ifndef PYSPAM_H
#define PYSPAM_H

#define PY_SSIZE_T_CLEAN
#define ODB_STRLEN 8  // 8 chars + '\0' 
#include <numpy/arrayobject.h>
#include <numpy/ndarraytypes.h>

//#define  NMDI = -2147483647   
//#define  RMDI = -2147483647.0 


#include  <stdio.h> 	
#include  <Python.h>
#include  "odbdump.h"
#define SMAX 50 



//  The unclude file could be renamed diffently , BUT  it stays  "pyspam.h"
//  since with a module called  pyspam.c  that i learned writing 
//  C extension for py  :-)  ... always remember !
//  Link could be found here :
//  https://docs.python.org/3/extending/extending.html  
//  Valid the : 11/03/2026




// BUILD THE DATA TYPE STRING 
/* static PyObject* makeString_data (char *data, npy_intp n, int N) {
   // FOR Python >= 3.12 
    PyArray_Descr *descr = PyArray_DescrNewFromType(NPY_STRING);
    #if NPY_ABI_VERSION < 0x2000000
         descr->elsize = N;
        #else
         Py_SET_SIZE((PyObject *)descr, N); // dernier recours pour versions >=2.0
     #endif
    if (!descr) return NULL;
    npy_intp dims[1] = { n };
    // Strides NULL => contigu with stride = elsize = 8
    PyObject *arr = PyArray_NewFromDescr(&PyArray_Type, descr,1, dims, NULL,(void*)data, 0, NULL);
    return arr;
}*/




/*    Destroy np Capsule   */
/*static void free_when_done(PyObject *capsule) {
    void *ptr = PyCapsule_GetPointer(capsule, NULL);
    if (ptr) {
       free(ptr);
    }
}*/


// Convert boolean PyObject  (from python True /False to C true/false)
static inline Bool PyObj_ToBool(PyObject *obj, Bool default_value)
{
    if (obj == NULL || obj == Py_None)
        return default_value;

    int truth = PyObject_IsTrue(obj);
    if (truth == -1) {          
        PyErr_Clear();         
        return default_value;
    }

    return (Bool)(truth != 0);
}




// Formatting floats
double  format(double value , char *fmt ) {
static  char str [SMAX ] ;
double    x  ;
sprintf(str, fmt , value );
x = atof(str );
return   x ;
}


static inline double format_float  (double val, int precision) {
    if (!isfinite(val)) return val;

    // Ensure decimal are between  0 and 15  
    if (precision <= 0) {
        printf("%s\n", "--WARNING : The number of decimal digits must >= 0 . -->  Fallback to default : fmt_float=15" ) ;
        precision = 15;
    }
    if (precision > 15) {
        printf("%s\n", "--WARNING : The number of decimal digits greater than 15. -->  Fallback to default : fmt_float=15" ) ;
        precision = 15;
    }
    char fmt[16];
    snprintf(fmt, sizeof(fmt), "%%.%df", precision);
    char buffer[64];
    snprintf(buffer, sizeof(buffer), fmt, val);
    return atof(buffer);
}


char* concat(const char *s1, const char *s2)
{
    char *result = malloc(strlen(s1) + strlen(s2) + 1); // +1 for the null-terminator
    strcpy(result, s1);
    strcat(result, s2);
    return result;
}


void strtonum(char [], int);
void strtonum(char str[], int num)
{
    int i, rem, len = 0, n; 
    n = num;
    while (n != 0)
    {
        len++;
        n /= 10;
    }
    for (i = 0; i < len; i++)
    {
        rem = num % 10;
        num = num / 10;
        str[len - (i + 1)] = rem + '0';
    }
    str[len] = '\0';
}

int get_strlen (  const char *  string  ) {  int str_len  ;
if ( string  ) { str_len=  strlen( string ); } else  { str_len=0   ;  };
return str_len ;
}
#endif 

