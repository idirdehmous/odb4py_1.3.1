#define PY_SSIZE_T_CLEAN

#include <Python.h>
#include <numpy/arrayobject.h>
#include <math.h>
#include <float.h>
#define PYPRINT(o)  PyObject_Print(o, stdout, 0); printf("\n");

// Define Pi ifndef 
#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

// Convert Degree to Radian
#define DEG2RAD(deg) ((deg) * M_PI / 180.0)

// Convert Radian to Degree
#define RAD2DEG(rad) ((rad) * 180.0 / M_PI)

// Power 
#define POWDI(x,i) pow(x,i)

#include "sql_build.h"




void sp_gcdist(double *lon1, double *lon2, double *lat1, double *lat2, double *dist)
{
    double F, G, L, sinG2, cosG2, sinF2, cosF2, sinL2, cosL2, S, C;
    double w, R, a, f, D, H1, H2;
    double lat1R, lat2R, lon1R, lon2R, DE2RA;

    DE2RA = M_PI / 180.0;
    a = 6378.137;              /* WGS-84 radius (km) */
    f = 1.0 / 298.257223563;   /* flattening */

    /* quick return if coordinates identical */
    if (fabs(lat1[0] - lat2[0]) < DBL_EPSILON &&
        fabs(fmod(lon1[0] - lon2[0], 360.0)) < DBL_EPSILON) {
        dist[0] = 0.0;
        return;
    }

    lat1R = lat1[0] * DE2RA;
    lat2R = lat2[0] * DE2RA;
    lon1R = lon1[0] * DE2RA;
    lon2R = lon2[0] * DE2RA;

    F = (lat1R + lat2R) / 2.0;
    G = (lat1R - lat2R) / 2.0;
    L = (lon1R - lon2R) / 2.0;

    sinG2 = sin(G) * sin(G);
    cosG2 = cos(G) * cos(G);
    sinF2 = sin(F) * sin(F);
    cosF2 = cos(F) * cos(F);
    sinL2 = sin(L) * sin(L);
    cosL2 = cos(L) * cos(L);

    S = sinG2 * cosL2 + cosF2 * sinL2;
    C = cosG2 * cosL2 + sinF2 * sinL2;

    /* safeguard */
    if (C <= 0.0 || S < 0.0) {
        dist[0] = 0.0;
        return;
    }

    w = atan(sqrt(S / C));
    if (w == 0.0) {
        dist[0] = 0.0;
        return;
    }

    R = sqrt(S * C) / w;
    D = 2.0 * w * a;
    H1 = (3.0 * R - 1.0) / (2.0 * C);
    H2 = (3.0 * R + 1.0) / (2.0 * S);

    dist[0] = D * (1.0 + f * (H1 * sinF2 * cosG2 - H2 * cosF2 * sinG2));
}




void sp_dists(double *u, double *v, double *uout, double *vout, int *n, double *dists, int *lonlat) {
        int N = *n, j;
        double gc[1];   

        if (lonlat[0] == 0) {
                for (j = 0; j < N; j++)
                        dists[j] = hypot(u[j] - uout[0], v[j] - vout[0]);
        } else {
                for (j = 0; j < N; j++) {
                        sp_gcdist(u+j, uout, v+j, vout, gc);
                        dists[j] = gc[0];
                }
        }
}



static PyObject* odb_gcdist_method( PyObject* Py_UNUSED(self) , PyObject* args)
{
    PyObject      *lon1_obj  , *lat1_obj , *lon2_obj, *lat2_obj;
    PyArrayObject *lon1      , *lat1     , *lon2    , *lat2    ;


    if (!PyArg_ParseTuple(args, "OOOO", &lon1_obj,
			                &lat1_obj,
					&lon2_obj,
					&lat2_obj)){ 
        
	PyErr_SetString(PyExc_RuntimeError, "--pyodb : Failed to parse odbGcdistance arguments ");
        return NULL;  
    }

   
   if ( !lon1_obj || !lat1_obj || !lon2_obj  || !lat2_obj  ) {
      PyErr_SetString(PyExc_RuntimeError, "--pyodb : 4 Arguments are required");
      return NULL;    
   }



    // Parse args and DECREF if it fails 
    lon1 = (PyArrayObject*)PyArray_FROM_OTF( lon1_obj, NPY_DOUBLE, NPY_ARRAY_IN_ARRAY);
    if (!lon1) {  
	PyErr_SetString(PyExc_RuntimeError, "--pyodb: Failed to parse lon1");    
	return NULL;
    }

    lat1 = (PyArrayObject*)PyArray_FROM_OTF(lat1_obj, NPY_DOUBLE, NPY_ARRAY_IN_ARRAY);
    if (!lat1) {
       Py_DECREF(lon1);
       PyErr_SetString(PyExc_RuntimeError, "--pyodb: Failed to parse lat1");
       return NULL;
       }

    lon2 = (PyArrayObject*)PyArray_FROM_OTF(lon2_obj, NPY_DOUBLE, NPY_ARRAY_IN_ARRAY);
    if (!lon2) {
       Py_DECREF(lon1);
       Py_DECREF(lat1);
       PyErr_SetString(PyExc_RuntimeError, "--pyodb: Failed to parse lon2");
       return NULL;
      }


    lat2 = (PyArrayObject*)PyArray_FROM_OTF( lat2_obj, NPY_DOUBLE, NPY_ARRAY_IN_ARRAY);
    if (!lat2) {
    Py_DECREF(lon1);
    Py_DECREF(lat1);
    Py_DECREF(lon2);
    PyErr_SetString(PyExc_RuntimeError, "--pyodb: Failed to parse lat2");
    return NULL;
    }


    // Set dimensions  [n x n ] matrix    
    int n1 = (int)PyArray_DIM(lat1, 0);
    int n2 = (int)PyArray_DIM(lat2, 0);
    
    npy_intp dims[2] = {n1, n2};
    PyArrayObject *distmat = (PyArrayObject*)PyArray_SimpleNew(2, dims, NPY_DOUBLE);

    if (!distmat) {
    Py_DECREF(lat1);
    Py_DECREF(lon1);
    Py_DECREF(lat2);
    Py_DECREF(lon2);
    PyErr_SetString(PyExc_RuntimeError, "--pyodb : Unable to allocate result for the contigous matrix.");
    return NULL;
    }
    
    double *latdata1 = (double*)PyArray_DATA(lat1);
    double *londata1 = (double*)PyArray_DATA(lon1);
    double *latdata2 = (double*)PyArray_DATA(lat2);
    double *londata2 = (double*)PyArray_DATA(lon2);
    double *distdata = (double*)PyArray_DATA(distmat);

    for (int i = 0; i < n1; ++i)
        for (int j = 0; j < n2; ++j) {
            double d;
            sp_gcdist(&londata1[i], &londata2[j], &latdata1[i], &latdata2[j], &d);
            distdata[i * n2 + j] = d;
        }
    
    Py_DECREF(lat1);
    Py_DECREF(lon1);
    Py_DECREF(lat2);
    Py_DECREF(lon2);
    return (PyObject*)distmat;
}









// Get  geopoints :  lat/lon alt date time and obsvalue 
static PyObject *odb_geopoints_method(PyObject *Py_UNUSED(self) , PyObject *args, PyObject *kwargs)
{
    char *database  = NULL;
    char *sql_cond  = NULL;
    char *unit      = "degrees";
    int fmt_float = 15;

    Bool lpbar   = false;
    Bool verbose = false;

    // Object  
    PyObject *poolmask_obj =Py_None ; 
    PyObject *extent_obj   =Py_None;

    // Boolean  
    PyObject *pbar  = Py_False;
    PyObject *pverb = Py_False;


    static char *kwlist[] = {
        "database",
        "sql_cond",
        "unit",
        "extent",
	"poolmask",
        "fmt_float",	
        "pbar",
        "verbose",
        NULL
    };

    if (!PyArg_ParseTupleAndKeywords(
            args, kwargs,
            "s|ssOOiOO",
            kwlist,
            &database,
            &sql_cond,
            &unit,
            &extent_obj,
	    &poolmask_obj,
            &fmt_float,
            &pbar,
            &pverb))
        return NULL;


    
    // Conversion to boolean C variable
    lpbar   = PyObj_ToBool ( pbar , lpbar      ) ;
    verbose = PyObj_ToBool ( pverb , verbose   ) ;

     // Convert to string
    const char *poolmask_str = NULL;
    if (poolmask_obj != Py_None) {
        if (!PyUnicode_Check(poolmask_obj)) {
         PyErr_SetString(PyExc_TypeError, "poolmask must be a string.  ex: '1 2 3 N' or '1:N' N=Number of pools'  \n") ;
         return NULL;
         }
     poolmask_str  = PyUnicode_AsUTF8(poolmask_obj);
    }


    // Number of functions in SQL 
    int  nfunc = 0 ;
    // Keywords dict for fetch  
    PyObject *key_args = PyDict_New();


// Declare a new sl statement 
SQLBuilder *sqlb = sqlbuilder_new();

if (!sqlb)
    return PyErr_NoMemory();


// The geo point first select statement 
// If in degrees 
if (unit && strcmp(unit,"degrees")==0){
sqlbuilder_add(sqlb,  "SELECT degrees(lat), degrees(lon),vertco_reference_1,vertco_reference_2,date,time, obsvalue FROM hdr, body WHERE 1=1");
nfunc  = 2  ;   
} else if (unit && strcmp(unit,"radians")==0  ) {
sqlbuilder_add(sqlb,  "SELECT lat, lon,vertco_reference_1,vertco_reference_2,date,time, obsvalue FROM hdr, body WHERE 1=1");
nfunc =0  ; 
}

// Get a subdomain  
if (extent_obj != Py_None) {
    if (!PySequence_Check(extent_obj) || PySequence_Size(extent_obj) != 4)
    {
        PyErr_SetString(PyExc_ValueError, "--odb4py : extent must be a list : [lon1, lon2, lat1, lat2]");
        return NULL;
    }

    // Get bbox 
    double lon1 = PyFloat_AsDouble(PySequence_GetItem(extent_obj,0));
    double lon2 = PyFloat_AsDouble(PySequence_GetItem(extent_obj,1));
    double lat1 = PyFloat_AsDouble(PySequence_GetItem(extent_obj,2));
    double lat2 = PyFloat_AsDouble(PySequence_GetItem(extent_obj,3));

  // Add SQL boundary box   
    sqlbuilder_addf(sqlb,
    " AND lat BETWEEN %.10g AND %.10g "
    " AND lon BETWEEN %.10g AND %.10g ",
    lat1, lat2, lon1, lon2);

}

// Add SQL parts 
//if (sql_cond && strlen(sql_cond) > 0) { 
//   sqlbuilder_addf(sqlb,  " AND  %s  ", sql_cond);
//}

// What if the  sql_cond is  empty   "  " or ""  
if (!is_blank_string(sql_cond)) {
    sqlbuilder_addf(sqlb, " AND (%s)", sql_cond);
}

// Get the SQL statement string and  free 
PyObject *sql_obj = sqlbuilder_build(sqlb);
sqlbuilder_free(sqlb);

if (!sql_obj)
    return NULL;
//const char *geo_sql = PyUnicode_AsUTF8(sql_obj);


// Insert to a dict object  
PyDict_SetItemString(key_args, "sql_query", sql_obj);

// Required 
PyDict_SetItemString(key_args, "database", PyUnicode_FromString(database));
PyDict_SetItemString(key_args, "nfunc",PyLong_FromLong(nfunc ));

// Optional  args 
// Format floats
PyDict_SetItemString(key_args, "fmt_float",PyLong_FromLong(fmt_float));

// Poolmask
PyDict_SetItemString(key_args, "poolmask"  ,poolmask_obj       );

// Progress bar  
PyDict_SetItemString(key_args, "pbar",     lpbar   ? Py_True : Py_False);

// Verbosity 
PyDict_SetItemString(key_args, "verbose",  verbose ? Py_True : Py_False);

// Quick print of the PyObject dict //PyObject_Print( key_args , stdout , 0 ) ; 


// Create empty tuple as posional arguments 
PyObject *args_call = PyTuple_New(0);

PyObject *rows   = odb_dict_method  ( NULL , args_call , key_args )  ;  

if (!rows ) {
    Py_DECREF(args_call);
    return NULL;
} else {
 return rows   ;  
}

}
