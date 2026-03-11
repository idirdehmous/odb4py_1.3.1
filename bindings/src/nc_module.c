#define PY_SSIZE_T_CLEAN

#include <numpy/arrayobject.h>
#include <numpy/ndarraytypes.h>

#include <stdio.h>
#include <ctype.h>
#include <math.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <Python.h>
#include "pyspam.h"
#include "rows.h"
#include "ncdf.h"

// Length of strings in ODB
#define ODB_STRLEN 8  // 8 chars + '\0'
 


// Get the returned rows and encode into NetCDF 
static int rows4nc(char *database,
                   char *sql_query,
                   int   fcols,
                   char *poolmask,
                   double **buffer,   // no copy  
                   char ***strbufs,   // no copy  
                   int *nrows,
                   int *ncols,
                   nc_column_t **cols,
                   colinfo_t   **ci , 
		   Bool lpbar , 
	           int fmt_float 	)
{
//    int fmt_float = 15;
    int maxcols   = 0;
    void *h       = NULL;
    int new_dataset = 0;
    int nci = 0;
    int nd;
    int ncols_all = 0;
    int row_idx   = 0;
    size_t ip       = 0 ;
    size_t prog_max = 0 ;

    int total_rows = getMaxrows(database, sql_query, poolmask);
    if (total_rows <= 0) {
        printf("--odb4py : ODB query returned zero rows\n");
        return -1;
    }

    h = odbdump_open(database, sql_query, NULL, NULL, NULL, &maxcols);
    if (!h || maxcols <= 0) {  printf("--odb4py : Failed to open ODB\n");
        return -1;
    }

    double *d = malloc(sizeof(double) * maxcols);
    if (!d) {
        odbdump_close(h);
        return -1;
    }

    while ((nd = odbdump_nextrow(h, d, maxcols, &new_dataset)) > 0) {
        if (lpbar) {  
	    ++ip;            
	    print_progress(ip, prog_max); 
	}   // useful for huge ODBs 
									 
        if (new_dataset) {
            *ci = odbdump_destroy_colinfo(*ci, nci);
            *ci = odbdump_create_colinfo(h, &nci);

            *cols    = malloc(sizeof(nc_column_t) * nci);
            *strbufs = calloc(nci, sizeof(char*));
            if (!*cols || !*strbufs)
                goto mem_error;

// Realloc if necessary   ( row_idx greater than tot_rows. Worth doing it  ?? )
if (row_idx >= total_rows) {
         total_rows *= 2;	
	 size_t size = (size_t)total_rows * (*ncols);
         *buffer     = realloc(*buffer, sizeof(double)*  size  );
        for (int j=0;j<*ncols;j++) {
           if ((*cols)[j].meta->dtnum == DATATYPE_STRING) {
              (*strbufs)[j] = realloc((*strbufs)[j],total_rows*(ODB_STRLEN+1));
        }
    }
}              
            // depends if a column is numeric or str 
            ncols_all = 0;
            for (int i = 0; i < nci; i++) {
                (*cols)[ncols_all].odb_col = i;
                (*cols)[ncols_all].meta    = &(*ci)[i];

                if ((*ci)[i].dtnum == DATATYPE_STRING) {
                    (*strbufs)[ ncols_all ] = calloc(total_rows, ODB_STRLEN + 1);
                    if (!(*strbufs)[ncols_all])
                        goto mem_error;
                      }
                ncols_all++;
            }
            *buffer = malloc(sizeof(double) * total_rows * ncols_all);
            if (!*buffer)
                goto mem_error;
            new_dataset = 0;
        }
        for (int j = 0; j < ncols_all; j++) {
            int i = (*cols)[j].odb_col;
            colinfo_t *pci = (*cols)[j].meta;
            double val = d[i];
            if (pci->dtnum == DATATYPE_STRING) {
                char *dst =
                    &(*strbufs)[j][row_idx * (ODB_STRLEN + 1)];
                if (fabs(val) == mdi) {
                    memset(dst, 0, ODB_STRLEN + 1);
                    strncpy(dst, "NULL", ODB_STRLEN);
                }
                else {
                    union {
                        char s[sizeof(double)];
                        double d;
                    } u;
                    u.d = d[i];
                    memcpy(dst, u.s, ODB_STRLEN);
                    dst[ODB_STRLEN] = '\0';
                    make_printable(dst, ODB_STRLEN);
                }
            }
            else {
                if (!val || fabs(val) == mdi) {
                    (*buffer)[row_idx*ncols_all + j] = NAN;
                }
                else {
                    switch (pci->dtnum) {
                        case DATATYPE_INT1:
                        case DATATYPE_INT2:
                        case DATATYPE_INT4:
                        case DATATYPE_YYYYMMDD:
                        case DATATYPE_HHMMSS:
                            (*buffer)[row_idx*ncols_all + j] =(double)(int)d[i];
                            break;
                        default:
                            (*buffer)[row_idx*ncols_all + j] =format_float(d[i], fmt_float);
                    }
                }
            }
        }

        row_idx++;
    }// while 
    *nrows = total_rows;
    *ncols = ncols_all;
    free(d);
    odbdump_close(h);
    return 0;

// errors Label 
mem_error:
    if (*buffer) free(*buffer);
    if (*strbufs) {
        for (int i = 0; i < ncols_all; i++)
            free((*strbufs)[i]);
           free(*strbufs);
    }
    if (*cols) free(*cols);
    if (*ci) odbdump_destroy_colinfo(*ci, nci);
    if (h) odbdump_close(h);
    if (d) free(d);
    return -1;
}





// Write into  NetCDF file                                    
static int writeNetcdf(const char *database   ,
		       const char *outfile    ,
		       char       *sql_query  ,  
                       double     *buffer     ,
		       char       *strbufs , 
                       int         nrows   ,
                       int         ncols   ,
                       nc_column_t *col, 
		       Bool verbose )
{


//  Just quick  Debug  
/*printf("---- STRING BUFFER ----\n");
for (int r = 0; r < nrows; r++)
{    char (*s)[ODB_STRLEN] = (char (*)[ODB_STRLEN])strbufs;
    printf("%.*s\n", ODB_STRLEN, s[r]);
//    printf("%.*s\n", ODB_STRLEN, strbufs + r*ODB_STRLEN);
}*/


// Local date & Time 
char datetime[64];
time_t now = time(NULL);
struct tm *tm_info = gmtime(&now);   // localtime()
strftime(datetime, sizeof(datetime), "%Y-%m-%d %H:%M:%S UTC", tm_info);

// Analysis & creation datetime  (From .dd file )
int  size = 512       ; 
char ddfile [ size ]  ;
build_dd_path (database ,  ddfile  , size  ) ; 

int vers  =0, majv =0 ;   // ODB version  realease  
int ana_date  =0 ;        // analysis datetime
int ana_time  =0 ; 
int creat_date=0 ;        // creatime datetime 
int creat_time=0 ;
int npools    =0 ;        // npools 
int ntabs     =0 ;        // number of considered tables   (ODB_CONSIDER_TABLES env var  in ODB user guide )
// Read  $dbname.dd file  
scan_dd_file ( database  , &vers , &majv , 
		           &ana_date , 
		           &ana_time , 
			   &creat_date , 
			   &creat_time ,
			   &npools     ,
			   &ntabs    )  ;
/*printf( "Path to dd file %s , ODB version %d.%d ,   analysis date-time %6.6d-%6.6d  ,  create date-time  %6.6d-%6.6d  npools=%d, ntables=%d\n" , 
		                                             ddfile ,  
							     vers , majv,
							     ana_date , 
							      ana_time ,
                                                              creat_date,
                                                              creat_time,
                                                              npools, 
                                                              ntabs    )  ;  */

// Prepare datetime for writing  
char ana_str  [32];
char creat_str[32];
format_datetime(ana_date, ana_time, ana_str);
format_datetime(creat_date, creat_time, creat_str);


// dims and vars 
    int ncid, dimid;
    int *varids = NULL;
    int check ;
    double fill = 0.0 / 0.0;   // fill missing values 

// create the file 
check = nc_create(outfile, NC_NETCDF4, &ncid);
if (check != NC_NOERR) { ERR(check);    return -1;  }

// NC conventions     
check  =  nc_put_att_text(ncid, NC_GLOBAL,"Conventions", strlen("CF-1.10"), "CF-1.10");
if (check != NC_NOERR) { ERR(check);    return -1;  }

// The title & global attrib 
const char *title       = "ODB data in NetCDF format";
const char *history     = "created by odb4py";
const char *institution = "(RMI) Royal Meteorological Institute of Belgium";
const char *feature     = "point" ; 
const char *data_source = "ECMWF ODB";
const char *encoding    = "ODB (row-major)" ; 
check  =nc_put_att_text(ncid, NC_GLOBAL, "Title"      ,strlen(title)       , title);
   if (check != NC_NOERR) { ERR(check);    return -1;  }
check  =nc_put_att_text(ncid, NC_GLOBAL, "History"    ,strlen(history)     , history);
   if (check != NC_NOERR) { ERR(check);    return -1;  } 
check  =nc_put_att_text(ncid, NC_GLOBAL, "Institution",strlen(institution) , institution);
   if (check != NC_NOERR) { ERR(check);    return -1;  } 
check  =nc_put_att_text(ncid, NC_GLOBAL, "Native_fomrat" ,strlen(data_source) , data_source );
   if (check != NC_NOERR) { ERR(check);    return -1;  }
check  =nc_put_att_text(ncid, NC_GLOBAL, "Encoding"   ,strlen(encoding)    , encoding);
   if (check != NC_NOERR) { ERR(check);    return -1;  }
check  =nc_put_att_text(ncid, NC_GLOBAL, "sql_query"  ,strlen(sql_query)   , sql_query);
   if (check != NC_NOERR) { ERR(check);    return -1;  }
check  =nc_put_att_text(ncid, NC_GLOBAL, "featureType",strlen(feature )    , feature );
   if (check != NC_NOERR) { ERR(check);    return -1;  }

// Date and time  
check  =nc_put_att_text(ncid, NC_GLOBAL, "NetCDF_datetime_creation",strlen(datetime) , datetime );
   if (check != NC_NOERR) { ERR(check);    return -1;  }
check  =nc_put_att_text(ncid, NC_GLOBAL, "analysis_datetime"       , strlen(ana_str)  , ana_str  );
   if (check != NC_NOERR) { ERR(check);    return -1;  }
check  =nc_put_att_text(ncid, NC_GLOBAL, "creation_datetime"       , strlen(creat_str), creat_str);
   if (check != NC_NOERR) { ERR(check);    return -1;  }

// The current odb attributes 
check  =nc_put_att_int(ncid, NC_GLOBAL, "version"      , NC_INT, 1, &vers);
   if (check != NC_NOERR) { ERR(check);    return -1;  }
check  =nc_put_att_int(ncid, NC_GLOBAL, "major_version", NC_INT, 1, &majv);
   if (check != NC_NOERR) { ERR(check);    return -1;  }
check  =nc_put_att_int(ncid, NC_GLOBAL, "npools"       , NC_INT, 1, &npools);
   if (check != NC_NOERR) { ERR(check);    return -1;  }
check  =nc_put_att_int(ncid, NC_GLOBAL, "ntables"      , NC_INT, 1, &ntabs);
   if (check != NC_NOERR) { ERR(check);    return -1;  }



// dims :  1D dimension whith length = nobs 
check  =nc_def_dim(ncid, "nobs", nrows, &dimid);  
   if (check != NC_NOERR) { ERR(check);    return -1;  }

// strings  a 2d   ( str_length , nobs  )
int strlen_dimid;
size_t str_len = ODB_STRLEN;
check = nc_def_dim(ncid, "strlen", str_len, &strlen_dimid);
if (check != NC_NOERR) { ERR(check);    return -1;  }

// Vars allocation 
varids = malloc(sizeof(int) * ncols);
if (!varids) return -1;

// Whether to write strings or numeric values 
// the nc_colname_t  structure is here  !
for (int i = 0; i < ncols; i++) {
        const char *name = col[i].meta->nickname ? col[i].meta->nickname: col[i].meta->name;
        char varname[128];
        strncpy(varname, name , sizeof(varname)-1);
        varname[sizeof(varname)-1] = '\0';
        sanitize_name(varname);
	// define vars & attributes 	
	if (strcmp(varname,"lat")==0 || strcmp(varname,"latitude")==0) {
         check = nc_put_att_text(ncid, varids[i], "units", strlen("degrees_north"),"degrees_north");
	 if (check != NC_NOERR) { ERR(check);    return -1;  }
         check =nc_put_att_text(ncid, varids[i],"standard_name",strlen("latitude"),"latitude");           
	 if (check != NC_NOERR) { ERR(check);    return -1;  }
	} // lat  

        if (strcmp(varname,"lon")==0 || strcmp(varname,"longitude")==0) {
         check=nc_put_att_text(ncid, varids[i], "units", strlen("degrees_east"), "degrees_east");
	 if (check != NC_NOERR) { ERR(check);    return -1;  }
         check=nc_put_att_text(ncid, varids[i], "standard_name", strlen("longitude"), "longitude");           
         if (check != NC_NOERR) { ERR(check);    return -1;  }
	} // lon 

if (col[i].meta->dtnum == DATATYPE_STRING) {
    int dimids[2] = {dimid, strlen_dimid};
    check = nc_def_var(ncid, varname, NC_CHAR, 2, dimids, &varids[i]);
    if (check != NC_NOERR) { ERR(check);    return -1;  }

} else {
    // Other variables
    int dimids[1] = {dimid};
    check = nc_def_var(ncid, varname, NC_DOUBLE, 1, dimids, &varids[i]);
    if (check != NC_NOERR) { ERR(check);    return -1;  }

    // long variable names ( @table   is not ommitted ) 
    check = nc_put_att_text(ncid, varids[i],"long_name",strlen(col[i].meta->name), col[i].meta->name);
    if (check != NC_NOERR) { ERR(check);    return -1;  }


// Missing values 
check =nc_put_att_double(ncid, varids[i],"_FillValue", NC_DOUBLE, 1, &fill);
   if (check != NC_NOERR) { ERR(check);    return -1;  }

} //   column type test
}  // for i loop
 
// End definition 
check=nc_enddef(ncid);
   if (check != NC_NOERR) { ERR(check);    return -1;  }


//write strings 
for (int c = 0; c < ncols; c++) {
    if (col[c].meta->dtnum == DATATYPE_STRING) {
        char *src = strbufs;
        size_t start[2] = {0,0};
        size_t count[2] = {nrows, ODB_STRLEN};
        check=nc_put_vara_text(ncid, varids[c], start, count, src);
	if (check != NC_NOERR) { ERR(check);    return -1;  }
    }
}

// Buffer numeric  
size_t start[1] = {0};
size_t count[1] = {nrows};
for (int c = 0; c < ncols; c++) {
    if (col[c].meta->dtnum == DATATYPE_STRING)
        continue;
    double *colbuf = malloc(sizeof(double) * nrows);
    if (!colbuf) {  return -1 ; }

    for (int r = 0; r < nrows; r++) {
        colbuf[r] = buffer[r*ncols + c];
       //  to netcdf  
       check=nc_put_vara_double(ncid, varids[c], start, count, colbuf);
      if (check != NC_NOERR) { ERR(check);    return -1;  }
  }
    free(colbuf);

}

// get the number of written bytes 
int nvars;
nc_inq_nvars(ncid, &nvars);
size_t total = 0;
for (int v = 0; v < nvars; v++)
    total += nc_var_size_bytes(ncid, varids[v] );

// 
if (verbose  ) {
printf( "%s\n" ,  "List of written columns :"  ) ;  
      for ( int c =0; c< ncols ; c++ ){
      printf  ( "%s  :  %s \n" , "Column " ,	 col[c].meta->name )  ; 
      }
}

// Close ncfile 
check=nc_close(ncid);
      if (check != NC_NOERR) { ERR(check);    return -1;  }          
    free(varids);

//  if is here without issue so ...
   if (verbose ) {
   printf("ODB data have been successfully written to NetCDF file : %s\n", outfile)  ; 
   printf("Total written data size = %zu bytes\n", total); 
   }
    return 0;
}





// Main function wrapper to perform conversion ODB--> NetCDF
static PyObject *odb2nc_method(PyObject *Py_UNUSED(self), PyObject *args, PyObject *kwargs){
    char *database  = NULL;
    char *sql_query = NULL;
    char *ncfile    = NULL;
    int   fcols     = 0;
    int  fmt_float  =15  ;    //default  
    Bool ldegree = true;
    Bool lpbar   = false;
    Bool verbose = false;
    static char *kwlist[] = {
        "database" ,
        "sql_query",
        "nfunc"    ,
        "ncfile"   ,
        "lalon_deg",
	"fmt_float",
        "pbar"     ,
        "verbose"  ,
        NULL
    };
    PyObject *degree_obj = Py_True;
    PyObject *pbar  = Py_None;
    PyObject *pverb = Py_None;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs,
                                     "ssis|OiOO",
                                     kwlist,
                                     &database,
                                     &sql_query,
                                     &fcols,
                                     &ncfile,
                                     &degree_obj,
				     &fmt_float , 
                                     &pbar,
                                     &pverb))
        return PyLong_FromLong(-1);

    ldegree = PyObj_ToBool(degree_obj, ldegree);
    lpbar   = PyObj_ToBool(pbar, lpbar);
    verbose = PyObj_ToBool(pverb, verbose);
    double *buffer  = NULL;
    char   **strbufs = NULL;
    int nrows = 0;
    int ncols = 0;
    nc_column_t *cols = NULL;
    colinfo_t   *ci   = NULL;

    // Get rows 
    if (rows4nc(database, sql_query, fcols, NULL,
                &buffer, &strbufs,
                &nrows, &ncols,
                &cols, &ci,
                lpbar, fmt_float) != 0)
    {
        PyErr_SetString(PyExc_RuntimeError, "--odb4py : failed to get rows from ODB for NetCDF encoding");
        return PyLong_FromLong(-1);
    }

    //   Convert coordinates        
    //   If it's not in degrees , force conversion  
    //   ldegree is infered from  the sql query  
    if (!ldegree) {
        if (verbose)
            printf("Coordinates converted radians to degrees\n\n");
        convert_rad_to_deg(buffer, nrows, ncols, cols);
    }

    // Allocate buffer for string 
    size_t str_len = ODB_STRLEN;
    char *buffer_str  = malloc(nrows * str_len);
    if (!buffer_str) {
        PyErr_SetString(PyExc_RuntimeError,
            "--odb4py : Failed to allocate string buffer string for NetCDF encoding");
        return PyLong_FromLong(-1) ;
    }

    //  Get string values 
    for (int c = 0; c < ncols; c++) {
        if (cols[c].meta->dtnum == DATATYPE_STRING) {
            char *src = strbufs[c];
            char *dst = buffer_str;
            for (int r = 0; r < nrows; r++) {
                memcpy(dst, src, str_len);
                dst += str_len;
                src += str_len + 1;
            }

	    // Better to send the both types at once 
            /*writeNetcdf_string_column(
                ncfile,
                sql_query,
                c,
                col_buf,
                nrows,
                str_len,
                cols
            );*/
        }
    }

     printf("Writing ODB data into NetCDF file ...\n" )  ; 

    int  status=writeNetcdf( database ,  ncfile,sql_query,buffer, buffer_str , nrows, ncols, cols, verbose  ) ;
    if (status != 0 ) {
        PyErr_SetString(PyExc_RuntimeError,
            "--odb4py : Failed to write data into the NetCDF file" );
       return  PyLong_FromLong( -1 );
       }

    // Free  allocations 
    free(buffer_str);
    free(buffer);
    free(cols);

    // Free the columpn struct 
    if (ci)   { 
       odbdump_destroy_colinfo(ci, 0); 
      }
    

  return PyLong_FromLong(0)  ; 
}
