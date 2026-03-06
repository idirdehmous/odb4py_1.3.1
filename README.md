[![PyPI version](https://img.shields.io/pypi/v/odb4py.svg)](https://pypi.org/project/odb4py/)
[![Documentation Status](https://readthedocs.org/projects/odb4py/badge/?version=latest)](https://odb4py.readthedocs.io)
![Python](https://img.shields.io/pypi/pyversions/odb4py)

# odb4py 1.1.2 release

## Description

**odb4py** is a C/Python interface to read and query ECMWF ODB1 databases.<br>
It provides high-performance access to ODB data through a native C backend, with seamless integration into the Python scientific ecosystem.<br>
The package embeds a customized version of the ECMWF ODB software [odb_api_bundle-0.18.1-Source](https://confluence.ecmwf.int) and is distributed as **manylinux wheels**, requiring no external ODB installation.

---

## Features

- Native C backend based on ECMWF ODB1
- Support for IFS and ARPEGE ODB databases
- SQL-like query interface
- Fast data access with [NumPy/C API](https://numpy.org/doc/2.1/reference/c-api/index.html) and pandas integration
- Manylinux wheels (portable across Linux distributions)
- No runtime dependency on system ODB or ECMWF bundles

---

## Installation

The **odb4py** package can be installed from PyPI using `pip`:

```bash
pip install odb4py  
```

## Installation test 
`from odb4py import core   # The C extension` <br>
`from odb4py import utils  # The python module helper` 



## Requirements
Python ≥ 3.9 <br>
NumPy  ≥ 2.0 <br>
Linux system (manylinux2014 compatible)

## Scientific context
ODB (Observation DataBase) is a column-oriented database format developed at ECMWF
and widely used in numerical weather prediction systems such as IFS,ARPEGE and NWP limited are models<br>

**odb4py** is primarily designed for:<br>
- Meteorologists and atmospheric scientists (especially within the ACCORD consortium)<br>
- Operational and research environments
- Post-processing and diagnostic workflows
- The current package version focuses on read-only access and data extraction for scientific analysis.


## License
Apache License, Version 2.0. [See LICENSE for details ](https://www.apache.org/licenses/LICENSE-2.0).


odb4py incorporates components derived from the ECMWF ODB software.

The original source code has been modified to:
- expose functionality through a Python interface
- reduce the runtime footprint
- enable portable binary wheel distribution

All original copyrights remain with ECMWF.

## Acknowledgements
This project incorporates and is derived from the ECMWF ODB software. <br/>

ODB was developed at the European Centre for Medium-Range Weather Forecasts (ECMWF)
by [S. Saarinen et al](https://www.ecmwf.int/sites/default/files/elibrary/2004/76278-ifs-documentation-cy36r1-part-i-observation-processing_1.pdf). All rights to the original ODB software remain with ECMWF and their respective owners.

