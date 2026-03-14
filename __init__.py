# -*-coding: utf-8 -*-
# odb4py
# Copyright (C) 2026 Royal Meteorological Institute of Belgium (RMI)
#
# Licensed under the Apache License, Version 2.0

__version__ = "1.3.1"

# Ensure that the env is initialized whichever module is called !
from .utils.setting_env import OdbEnv
if not globals().get("_ODB_ENV_INITIALIZED", False):
    _env = OdbEnv()
    _env.init()
    _ODB_ENV_INITIALIZED = True
