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

import pysequoiadb
from pysequoiadb import common
from pysequoiadb.common import const

class SDBTypeError(TypeError):
   """Type Error of SequoiaDB
   """

class SDBBaseError(Exception):
   """Base Exception of Python Driver for SequoiaDB
   """
   def __init__(self, errmsg, code, type):

      self.__type    = type
      self.__errmsg  = errmsg
      self.__code    = code
      self.__details = None

      if code is not None and isinstance(code, int):
         try:
            self.__details = common.get_info(code)
         except KeyError:
            self.__details = None

      Exception.__init__(self, errmsg)

   def __repr__(self):

      return "%s: %s" % ( self.__type, self.__errmsg)

   def __str__(self):

      return self.__repr__()

   def __detail(self):
      """get detail info with code
      """
      return "Error code: %s, detail: %s" % (self.__code, self.__details)

   @property
   def code(self):
      """The error code returned by the server, if any.
      """
      return self.__code

   @property
   def detail(self):
      """return the detail error message
      """
      return self.__detail()

class SDBEndOfCursor(Exception):
   """Invalid Parameter Error
   """
   def __init__(self):
      Exception.__init__(self, "End of Cursor")

class SDBError(SDBBaseError):
   """Gerneral Error of SequoiaDB
   """
   def __init__(self, errmsg, code):
      SDBBaseError.__init__(self, errmsg, code, "SequoiaDB Error")

class SDBIOError(SDBBaseError):
   """IO Error of SequoiaDB
   """
   def __init__(self, errmsg, code):
      SDBBaseError.__init__(self, errmsg, code, "IO Error")

class SDBNetworkError(SDBBaseError):
   """Network Error of SequoiaDB
   """
   def __init__(self, errmsg, code):
      SDBBaseError.__init__(self, errmsg, code, "Network Error")

class InvalidParameter(SDBBaseError):
   """Invalid Parameter Error
   """
   def __init__(self, errmsg, code):
      SDBBaseError.__init__(self, errmsg, code, "Invalid Parameter")

class SDBSystemError(SDBBaseError):
   """System Error of SequoiaDB
   """
   def __init__(self, errmsg, code):
      SDBBaseError.__init__(self, errmsg, code, "System Error")

class SDBUnknownError(SDBBaseError):
   """Unknown Error of SequoiaDB
   """
   def __init__(self, errmsg):
      SDBBaseError.__init__(self, errmsg, None, "Unknown Error")

