#   Copyright (C) 2012-2014 SequoiaDB Ltd.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

from pysequoiadb import const
from pysequoiadb.enum import enum

NODE_STATUS = enum(((0,"ALL"),(1,"ACTIVE"),(2,"INACTIVE"),(3,"UNKNOWN")))

const.errmaps = dict()

# some error code
const.SDB_OK                     = 0
const.SDB_DMS_EOC                = -29

# io error
const.SDB_IO                     = -1
const.SDB_FNE                    = -4
const.SDB_FE                     = -5
const.SDB_NOSPC                  = -11

#network error
const.SDB_NETWORK                = -15
const.SDB_NETWORK_CLOSE          = -16
const.SDB_NET_ALREADY_LISTENED   = -77
const.SDB_NET_CANNOT_LISTEN      = -78
const.SDB_NET_CANNOT_CONNECT     = -79
const.SDB_NET_NOT_CONNECT        = -80
const.SDB_NET_SEND_ERR           = -81
const.SDB_NET_TIMER_ID_NOT_FOUND = -82
const.SDB_NET_ROUTE_NOT_FOUND    = -83
const.SDB_NET_BROKEN_MSG         = -84
const.SDB_NET_INVALID_HANDLE     = -85

# invalid error
const.SDB_INVALIDARG             = -6
const.SDB_INVALIDSIZE            = -7
const.SDB_INVALIDPATH            = -19
const.SDB_INVALID_FILE_TYPE      = -20

# system error
const.SDB_OOM                    = -2
const.SDB_SYS                    = -10

const.LOB_CREATE = 1
const.LOB_READ   = 4

def get_info(code):
   try:
      info = const.errmaps[code]
   except KeyError:
      raise

   return info

import os
import sys
import string

init=False

def init_errmaps():
   if False == os.access(os.path.dirname(__file__) + "/err.prop", os.R_OK):
      raise Exception("file not exist")
   import ConfigParser
   config = ConfigParser.ConfigParser()
   config.read(os.path.dirname(__file__) + "/err.prop")
   pairs = config.items("error")
   for pair in pairs:
      const.errmaps[ string.atoi(pair[0]) ] = pair[1];

if False == init:
   init_errmaps()
   init = True




