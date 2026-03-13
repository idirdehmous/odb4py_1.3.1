# -*- coding: utf-8 -*-
import os  , sys 
from sys import byteorder 
import glob  
import ctypes
import re

__all__=[ "SqlParser" ]

from   .exceptions               import *
from   .odb_glossary             import OdbLexic 



class SqlParser:
      def __init__(self ):
          pass 
#  FOR THE MOMENT DONE WITH SIMPLE regex (works pretty well !)
#  FROM   include/info.h  
#  int ncols;              /* total number of columns to be dealt with when executing SELECT */
#  int ncols_true;         /* True number of columns i.e. ncols_pure + ncols_formula + ncols_aggr_formula */
#  int ncols_pure;         /* no formula, pure column (kind=1) */
#  int ncols_formula;      /* simple formula (kind=2) */
#  int ncols_aggr_formula; /* aggregate formula (kind=4) */
#  int ncols_aux;          /* auxiliary cols needed due to aggregate functions (kind=8) */
#  int ncols_nonassoc;     /* column variables needed to be prefetched due to formula(s) (kind=0) */
#  int ncolaux;            /* Should be == ncols_true + ncols_aux */

      def get_nfunc (self, raw):
          fcnt=[]
          od  =OdbLexic ()
          kws =od.odb_sqlwords()                     # ODB/SQL WORDS
          func=od.odb_funcs   ()                     # ODB FUNCTIONS                    
          rr=raw.lower().split()                    # BE SURE THAT THE QUERY IS IN LOWER CASE 
          rj="".join(rr).partition('from')          # JOIN EVERYTHING AND USE "from" KEYWORD AS SEPARATOR
          tokens = rj[0:1]                          # THE SELECT STETEMENT IS AT INDEX 0 
          # GET built-in functions OCCURENCES WITH REGEX 
          # Temporarly lists 
          # FUNCTIONS NAMES ARE NOT CASE SENSITIVE IN THE ODB/SQL SYNTAX PARSER 
          # Tokens are already in lower case , the functions names should also be !
          # Counting the number of functions is not for fun !  
          # The ODB C code allocates N columns for pure select in query + N cols for the used functions ( they  should be sustracted from maxcols !)
          cnt=0
          for t in tokens:
              for f in func: 
                  fn=f.lower()
                  regex  =fn+r"s*\("
                  found =re.findall ( regex , t  ) 
                  n_occur=len(found )  
                  if n_occur  != 0:
                    cnt = cnt + n_occur                 
          return cnt  

      def in_degrees(self, raw):
          """
          Detect if the SQL query already contains a
          degrees() conversion.
          """
          query = raw.lower()
          # remove spaces for easier detection
          query = "".join(query.split())
          # look for degrees(
          found = re.search(r"degrees\s*\(", query)
          if found:
             return True
          return False


      def clean_string  (self , string ):
          # C characters not accepted in strings arguments 
          c_char='\0\a\b\f\n\r\r\t\v\\'
          cleaned_st= string.translate({ord(i): None for i in c_char})
          return cleaned_st

 
      def attrs_query  (self, sql_raw ):
          p = {}
          nfunc   =  self.parse_tokens (sql_raw )
          in_deg  =  self.in_degrees   (sql_raw )
          cleaned =  self.clean_string (sql_raw )
          p = {"nfunc": nfunc, "in_degrees": in_deg , "query":cleaned  }
          return p 
