#ifndef NCDF_H
#define NCDF_H

#include <libgen.h>
#include "netcdf.h"

// Define Pi 
#define PI 3.141592653589793
#define RAD2DEG (180.0/PI)


/* Handle errors by printing an error message and exiting with a
* non-zero status. from   https://docs.unidata.ucar.edu/netcdf-c  */
#define ERRCODE 2
#define ERR(e) {printf("Error: %s\n", nc_strerror(e)); exit(ERRCODE);}



typedef struct  {
    int odb_col;
    colinfo_t *meta;
    int is_string;
} nc_column_t;


void sanitize_name(char *name)
{    
for (char *p = name; *p; p++){
        if (*p=='(' || *p==')' ||
            *p=='@' || *p=='.' ||
            *p=='/' )
        {
            *p = '_';
        }
    }
}


static void make_printable(char *s, int slen)
{
    for (int j = 0; j < slen; j++) {
        unsigned char c = s[j];
        if (!isprint(c))
            s[j] = ' ';
    }
}

static int is_coord(const char *name)
{
    if (!name) return 0;
    return (!strcmp(name,"lat")  || !strcmp(name,"lon")  || !strcmp(name,"latitude") || !strcmp(name,"longitude"));
}


static void convert_rad_to_deg(double *buffer,
                                   int nrows,
                                   int ncols,
                               nc_column_t *cols)
{
    for (int c = 0; c < ncols; c++)
    {
        if (cols[c].is_string)
            continue;
        const char *name = cols[c].meta->name;
        char varname[128];
        strncpy(varname, name , sizeof(varname)-1);
        varname[sizeof(varname)-1] = '\0';
        sanitize_name(varname);
        if (!is_coord(varname))
            continue;
        for (int r = 0; r < nrows; r++)
        {
            int idx = r*ncols + c;
            buffer[idx] *= RAD2DEG;
        }
    }
}


void build_dd_path(const char *path, char *ddfile, size_t size)
{
    char tmp[512];
    strncpy(tmp, path, sizeof(tmp));
    tmp[sizeof(tmp)-1] = '\0';
    char *base = basename(tmp);
    char dbname[128];
    strncpy(dbname, base, sizeof(dbname));
    dbname[sizeof(dbname)-1] = '\0';

    char *dot = strchr(dbname,'.');
    if (dot)
        *dot = '\0';
    snprintf(ddfile, size, "%s/%s.dd", path, dbname);
}



void   scan_dd_file ( const char *database   , 
                             int *vers , 
			     int *majv ,
		             int *ana_date   , 
			     int *ana_time   ,
			     int *creat_date , 
			     int *creat_time ,
			     int *npools     , 
			     int *ntabs  ) {

char  ddfile [256] ;
char  line   [256];
int minv ;
int modif_date , modif_time  ;

int nfirst_line=6  ; 
build_dd_path(    database    ,ddfile , sizeof(ddfile));

FILE *fp = fopen(ddfile, "r");
if (!fp) {
    return  (void) NULL ;
}
int  i = 0  ;
while (fgets(line, sizeof(line), fp) && i <= ( nfirst_line -1)) {

    switch  (i)  {
      case 0: sscanf(line, "%d %d %d" , &(*vers) , &(*majv) , &minv      ); break  ;
      case 1: sscanf(line, "%d %d",    &(*creat_date) , &(*creat_time )  ); break  ;
      case 2: sscanf(line, "%d %d",  &modif_date, &modif_time     ); break  ;
      case 3: sscanf(line, "%d %d",  &(*ana_date  ), &(*ana_time) ); break  ;
      case 4: sscanf(line, "%d",  &(*npools )  ); break ;
      case 5: sscanf(line, "%d",  &(*ntabs)    ); break ;
      default:
	 (void) 0 ;   // do nothing  
    }
    i++  ;
}
}
// Format datetime to  YY-MM-DD HH:MM:SS UTC 
void format_datetime(int date, int time, char *out){
    int Y = date / 10000;
    int M = (date / 100) % 100;
    int D = date % 100;
    int h = time / 10000;
    int m = (time / 100) % 100;
    int s = time % 100;
    sprintf(out, "%04d-%02d-%02d %02d:%02d:%02d UTC", Y, M, D, h, m, s);
}

// Count number of written bytes 
size_t nc_var_size_bytes(int ncid, int varid)
{
    nc_type vartype;
    int ndims;
    int dimids[NC_MAX_VAR_DIMS];

    size_t typesize;
    size_t dimlen;
    size_t nelems = 1;

    int check  ;  
    check=nc_inq_vartype(ncid, varid, &vartype);
       if (check != NC_NOERR) { ERR(check);    return -1;  }
    check=nc_inq_varndims(ncid, varid, &ndims);
       if (check != NC_NOERR) { ERR(check);    return -1;  }
    check=nc_inq_vardimid(ncid, varid, dimids);
       if (check != NC_NOERR) { ERR(check);    return -1;  }
    check=nc_inq_type(ncid, vartype, NULL, &typesize);
       if (check != NC_NOERR) { ERR(check);    return -1;  }


    for (int i = 0; i < ndims; i++) {
        check=nc_inq_dimlen(ncid, dimids[i], &dimlen);
	   if (check != NC_NOERR) { ERR(check);    return -1;  }
        nelems *= dimlen;
    }
    return nelems * typesize;
}

#endif
