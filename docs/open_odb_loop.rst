Using odb4py inside loops
=========================

ODB databases are frequently used to perform diagnostics and statistical
analysis over a sequence of dates or time periods. In such workflows,
looping over dates is therefore a common and necessary practice.

During these iterations, both the database path and the associated date
must be updated at each step. If this is not done correctly, **odb4py may continue to operate on the first opened ODB database**. As a result,
identical rows and values may be returned even though the loop variable
(e.g. the date) changes.

.. warning::

   The ODB runtime keeps an internal state based on environment
   variables defining the active database. If these variables are not
   refreshed, odb4py may continue to access the first opened ODB
   database, even if the loop variable changes.

   To ensure that each iteration accesses the correct database, the
   following ODB environment variables must be updated accordingly
   inside the Python loop:

   - ``IOASSIGN``
   - ``ODB_SRCPATH_CCMA``   
   - ``ODB_DATAPATH_CCMA``  
   - ``ODB_IDXPATH_CCMA``

   The following variables have to be used in the case of an `ECMA` database.

   - ``ODB_SRCPATH_ECMA``   
   - ``ODB_DATAPATH_ECMA``
   - ``ODB_IDXPATH_ECMA``  

   Updating these variables forces the ODB runtime to reload the correct
   database context and prevents unintended reuse of a previously opened ODB.


Recommended workflow:

1. Update the database path and environment variables for the current date.
2. Open the database.
3. Execute the required queries.
4. Close the database before moving to the next iteration.

.. code-block:: python 

   # -*- coding: utf-8 -*-
   import os, sys
   from datetime import datetime

   # Import odb4py 
   from  odb4py.utils   import  OdbEnv ,  OdbObject  ,  StringParser
   from  odb4py.core    import  odbConnect, odbClose , odbDca , odbDict



   def Connect (db_path , db_name, ncpu ):
       iret  = odbConnect ( odbdir =db_path+"/"+db_name   )
       return iret

   def CreateDca (db_path ,db_name ,  NCPU=ncpu  ):
       # Create DCA if not there
       if not os.path.isdir (dbpath+"/dca"):
          ic =odbDca ( dbpath=db_path, db= db_name , ncpu=NCPU )
       return ic 



   def FetchData ( dbpath ,  query  ):
       # Check the  query
       p      =StringParser()
       nfunc  =p.ParseTokens ( query )    # Parse sql statement and get the number of functions
       sql    =p.CleanString ( query  )   # Check and clean before sending !
       nfunctions=nfunc
       query_file= None
       pool      = None
       
       # If the an error occurs while executing the query a RuntimeError Exception is raised !
       try:
          rows =odbDict (database  = dbpath    ,
                        sql_query = sql        ,
                        nfunc     = nfunctions ,
                        fmt_float = 10         ,
                        pbar      = True       ,
                        verbose   = False      ,
                        queryfile = None       ,
                        poolmask  = None  )                     
       except:
          RuntimeError
          print("Failed to get data from the ODB {}".format(dbpath) )
       return rows


    # Script start runtime
    start = datetime.now()

    # Path to ODB directories
    # e.g : /path/to/odb/YYYYMMDDHH/CCMA 
    # Let's use the same ODBs from MetCoOp domain
    odb_dir_location  = "/home/odb"

    # The SQL query
    sql_query="select statid,\
          degrees(lat)  ,\
          degrees(lon)  ,\
          varno         ,\
          obsvalue      ,\
          fg_depar      ,\
          an_depar       \
          FROM  hdr, body"

    # Set date/time period (20240110 00h00 --> 20240112 21h00 )
    yy= 2024
    mm= 1
    d1= 10
    d2= 12
    h1= 0
    h2= 21
    cycle_inc= 3
    dbname="CCMA"
    ncpu_dca= 4
    for d in range(d1,  d2 +1 ):
       for h in  range( h1 , h2 +1 , cycle_inc ):

          # Month , day and hour leading zero
          mm= "{:02}".format( mm )
          dd= "{:02}".format( d )
          hh= "{:02}".format( h )
          ddt=str(yy)+str(mm)+dd+hh

          # Set the path
          dbpath = "/".join( (odb_dir_location  , ddt , dbname)  )

          # Reset the paths and IOASSIGN environnment variables for each iteration
          os.environ["ODB_SRCPATH_CCMA" ]=dbpath
          os.environ["ODB_DATAPATH_CCMA"]=dbpath
          os.environ["ODB_IDXPATH_CCMA" ]=dbpath
          os.environ["IOASSIGN"  ]       =dbpath+"/"+"IOASSIGN"

          # Connect
          ic    = Connect  (dbpath ,dbname )
          
          # Create DCA 
          i_dca = CreateDca(dbpath ,dbname , ncpu_dca )

          # Get the data
          row_dict = FetchData  (dbpath , sql_query)

          # Close the database 
          odbClose()

    end = datetime.now()
    duration = end - start
    print( "Runtime duration :" , duration )

Runtime duration : 0:00:24.99

