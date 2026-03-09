#define PY_SSIZE_T_CLEAN
#include <ctype.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <Python.h>
#include "magicwords.h"
#include "info.h"
#include "odb.h"
#include "privpub.h"
#include "macros.h"
#include "iostuff.h"
#include "ansi_colors.h"



/* ENDIANESS */
extern int ec_is_little_endian();

// Global  
static int maxhandle = 0;     /* Actual max no. of handles; increase/decrease via export ODB_MAXHANDLE */
static  const int def_maxhandle = 10;
static  DB_t      *free_handles = NULL;

PUBLIC void *
ODBc_get_free_handles(int *Maxhandle)
{  if (Maxhandle) *Maxhandle = maxhandle;  return (void *)free_handles; }


// OPEN ODB  AND GET ODB handler !
static PUBLIC   PyObject*   odbConnect_method ( PyObject *Py_UNUSED(self) , PyObject* args ,  PyObject *kwargs)  {
	char *dbname  =NULL  ; 
        char *mode    =NULL  ;
	int  *npools  =0     ;
	int  *ntables =0     ; 

	
        if(dbname  ) FREE(dbname)   ;
        if(mode    ) FREE(mode)     ;
        if(npools  ) FREE(npools )  ;
        if(ntables ) FREE(ntables)  ;

        // Init  pyobj to None 
        PyObject *poolmask_obj = Py_None  ;
        PyObject *verbose      = Py_None  ;


	static char *kwlist[] = {"odbdir" , "verbose", "poolmask", NULL };
       if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|OO",      // dbpath is required , verbose and npools are optional 
                  			    kwlist ,
                                            &dbname ,
                                            &verbose, 
					    &poolmask_obj 
					    ))
       {
	PyErr_SetString(PyExc_ValueError, "--odb4py  odbConnect(): invalid arguments. Use odbConnect('/path to.../ODB').");
        return NULL;
       }

       // no verbosity  
       Bool      lverb        = false    ;

      if (verbose && PyObject_IsTrue(verbose)) {
        lverb = true;
      } else {
        lverb = false;
            }

    // Convert poolmask  to C string  
    const char *poolmask = NULL;
    if (poolmask_obj != Py_None) {
        if (!PyUnicode_Check(poolmask_obj)) {
         PyErr_SetString(PyExc_TypeError, "poolmask must be a string.  ex: '1 2 3 N' or '1:N' N=Number of pools'  \n") ;
         return NULL;
         }
     poolmask  = PyUnicode_AsUTF8(poolmask_obj);
      }


      Bool error = false;
      int handle = -1 ; 

    if (  dbname )  {
    int j;
    int Npools     = 0;
    int Ntables    = 0;
    DB_t *ph       = NULL;
    char *p_dbname = NULL;
    char *srcpath_dbname  = NULL;
    char *datapath_dbname = NULL;
    char *idxpath_dbname  = NULL;
    char *env ;
    Bool first_time = false;

    if (!free_handles) {
     char *  MAXH= "ODB_MAXHANDLE"   ; 
     env = getenv(MAXH );
      if (env) maxhandle = atoi(env);
      if (maxhandle <= 0) maxhandle = def_maxhandle;
      CALLOC(free_handles, maxhandle);
      /* Other first time setups */
      {
	extern Boolean iostuff_debug;
	iostuff_debug = false;
      }
      env = STRDUP("ODB_IO_METHOD=5"); /* Remains allocated ; cannot be free'd due to putenv() below */
      putenv(env);
      first_time = true;
    }

    for ( j=0; j  <maxhandle  ; j++) {
      if (free_handles[j].h == 0) {
	handle = j+1;
	ph = &free_handles[j];
	break;
      }
    }


    if (!ph) {
      fprintf(stderr, "--odb4py : Unable to open ODB '%s': too many open databases (max=%d).\n",
                dbname , maxhandle);
      fprintf( stderr ,
              "\tMax. no. of open databases currently = %d. Increase via export ODB_MAXHANDLE\n",
              maxhandle);
      handle = -1;
      return PyLong_FromLong(-1);
    }


    { /* Check if database name contains an '/' */
      char *pdot   = NULL;
      char *slash  = NULL;
      char *p = STRDUP(dbname);
      char *pdollar = strchr(p, '$');

      if (pdollar) {
	/* Resolve possible environment variable(s) in the "dbname" i.e. "p" */
	char *tmp = IOresolve_env(p);
	FREE(p);
	p = tmp;
      }

      if (strequ(p,".")) {
	/* "dbname" points to the current directory ? */
	char curpath[4096];
	FREE(p);
	p = STRDUP(getcwd(curpath, sizeof(curpath)));
      }


      {
	/* If last char(s) is/are '/', remove it/them from "dbname" i.e. "p" */
	int len = STRLEN(p);
	while (len >= 0 && p[len-1] == '/') {
	  p[len-1] = '\0';
	  --len;
	}
      }

      {
	/* Is "dbname" i.e. "p" a (schema-)file or a directory ? */
	struct stat buf;
	int    exist   = (stat(p, &buf) == -1) ? 0 : 1;
	Bool   is_dir  = (exist && S_ISDIR(buf.st_mode)) ? true : false;
	if (is_dir) {
	  /* Directory --> Append <db>.sch to "p" */
	  int len;
	  char *tmp = STRDUP(p);
	  char *db  = NULL;

	  slash     = strrchr(tmp, '/');

	  if (slash) {
	    db = STRDUP(slash+1);
	  }
	  else {
	    db = STRDUP(tmp);
	  }
	  pdot = strchr(db, '.');
	  if (pdot) *pdot = '\0';
	     const char suffix[] = ".sch";
//	     len = STRLEN(tmp) + 1 + STRLEN(db) + STRLEN(suffix ) + 1;
	     int ltmp = get_strlen(tmp) ;
             int ldb  = get_strlen(db)  ;
             //int lsuffix= get_strlen( suffix )
	     len = ltmp + 1 +  ldb + 4 + 1  ; 
	     FREE(p);
	     ALLOC(p, len);
	     snprintf(p, len, "%s/%s%s", tmp, db, suffix);
	  FREE(tmp);
	} /* if (is_dir) */
      }
      slash = strrchr(p, '/');
      if (slash) {
	*slash = '\0';
	 p_dbname = STRDUP(slash+1);
	 pdot = strchr(p_dbname, '.');
	if (pdot) *pdot = '\0';
	   srcpath_dbname  = STRDUP(p); 
	   datapath_dbname = STRDUP(p); 
	   idxpath_dbname  = STRDUP(p);
        }
      else {
	   pdot = strchr(p, '.');
	   if (pdot) *pdot = '\0';
	   p_dbname = STRDUP(p);
      }
      FREE(p);
    }

    ph->h      = handle;
    ph->dbname = p_dbname;
    SET_PATH(srcpath  , SRCPATH ,  NULL);
    SET_PATH(datapath , DATAPATH,  NULL);
    SET_PATH(idxpath  , IDXPATH , "/idx");
    ph->srcpath  = srcpath_dbname ;
    ph->datapath = datapath_dbname;
    ph->idxpath  = idxpath_dbname ;

    if (first_time) { 
      // The following is done once only, since currently IOASSIGN is upon the first cma_open() 
      // Check existence of IOASSIGN and if not defined, then define it 
      env = getenv("IOASSIGN");
      if (!env) {
	int len = 2*strlen("IOASSIGN=") + strlen(srcpath_dbname) + 1 + strlen(p_dbname) + 1;
	ALLOC(env, len);
	snprintf(env, len, "IOASSIGN=%s/%s.IOASSIGN", srcpath_dbname, p_dbname);
	putenv(env); 
      }
      env = getenv("IOASSIGN");
      codb_init_(NULL, NULL); 
      codb_init_omp_locks_();
      codb_trace_init_(NULL); 
      // Let poolmasking know what's the maxhandle 
      codb_alloc_poolmask_(&maxhandle);
      }  //if (first_time) 



    if (poolmask) { /* Set poolmask explicitly for this database */
      char        *env = NULL;
      const char str[] = "ODB_PERMANENT_POOLMASK";
      int len  ; 
      //int len          = STRLEN(str) + 1 + STRLEN(p_dbname) + 1 + STRLEN(poolmask) + 1;
      int lstr = get_strlen(str) ;
      int ldb  = get_strlen(p_dbname)  ;
      int lmsk = get_strlen( poolmask  ); 
      len = lstr + 1 +  ldb + lmsk + 1  ;
      ALLOC(env,len);
      snprintf(env, len, "%s_%s=%s", str, p_dbname, poolmask);
      putenv(env);
      /* FREE(env); (cannot be freed due to putenv()) */ 
    } /* if (poolmask) */

    // Open a given table 
    ph->tblname = NULL;
    { /* Read primary metadata (i.e. usually the $ODB_SRCPATH_<dbname>/<dbname>.dd -file) */
      int iret = 0;
      int iounit = -1;
      cma_open_ (&iounit, p_dbname , "r", &iret, strlen(p_dbname), 1);
      if (iret < 1) {
        fprintf(stderr, BRED "--odb4py : Could not open ODB '%s' (iret=%d)\n" reset,
                srcpath_dbname , iret);
        error = true;
             } 
      else{
	if (lverb ) {
         fprintf(stderr, BGRN "--odb4py : Successfully opened ODB '%s'\n" reset,  srcpath_dbname );
	}

       }
  
       // close unit  
      cma_close_(&iounit, &iret);


    }
    /* Initialize poolmask for this database */    
    codb_init_poolmask_(&handle, p_dbname, &Npools, strlen(p_dbname));
    ph->npools  = Npools;
    ph->ntables = Ntables;
    if (npools) *npools   = Npools;
    if (ntables) *ntables = Ntables;
  }

  // if error  (failed ) return -1 else return 0 
  if (error && handle >= 1) {
        ODBc_close(handle);
        return PyLong_FromLong(-1);
    } else 
   // if success return  0
    return PyLong_FromLong(0); 
}





// Close the ODB 
static PUBLIC PyObject* odbClose_method(PyObject *Py_UNUSED(self), PyObject *args)
{
    int handle = 0;
    // handle mandatory 
    if (!PyArg_ParseTuple(args, "i", &handle)) {
        PyErr_SetString(PyExc_ValueError,
            "--odb4py : odbClose(handle) expects the value `1` as an argument.");
        return NULL;
    }

    if (handle =! 1) { handle =1 ; }

   if   (!free_handles || handle < 1 || handle > maxhandle)  {
        fprintf(stderr,
            "--odb4py : invalid handle %d\n", handle);
        return PyLong_FromLong(-1);
    }

    
    DB_t *ph = &free_handles[handle - 1];

    //
    DCA_free(handle);
    codb_end_poolmask_(&handle);

    // Free allocated memory 

    FREE(ph->srcpath);
    FREE(ph->datapath);
    FREE(ph->idxpath);
    FREE(ph->dbname);

    if (ph->tblname && ph->ntables > 0) {
        for (int i = 0; i < ph->ntables; ++i)
            FREE(ph->tblname[i]);
        FREE(ph->tblname);
    }

    // ---->> reset the structures 
    ph->srcpath  = NULL;
    ph->datapath = NULL;
    ph->idxpath  = NULL;
    ph->dbname   = NULL;
    ph->tblname  = NULL;
    ph->ntables  = 0;
    ph->npools   = 0;
    ph->h        = 0;

    fprintf(stderr,"--odb4py : database closed.\n");

    return PyLong_FromLong(0);
}
