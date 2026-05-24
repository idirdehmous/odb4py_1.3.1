# -*-coding :utf-8 -*- 


"""
Check connection object 
"""

from odb4py.utils import OdbObject

# Import function
from odb4py.core import odb_open


# Path to a CCMA database
exo_path   ="../odb_samples/metcoop"
dbtype = "CCMA"
dbpath = "/".join(( exo_path  , dbtype))


# Open the database
conn  = odb_open  (database =dbpath )
print( conn )
