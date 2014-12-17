try:
   import sdb
except ImportError:
   raise Exception("Cannot find extension: sdb")

import bson
from bson.objectid import ObjectId
import pysequoiadb

from pysequoiadb.cursor import cursor
from pysequoiadb import error
from pysequoiadb.common import const
from pysequoiadb.error import (SDBBaseError, SDBTypeError, SDBSystemError,
                               InvalidParameter)

class lob(object):

   def __init__(self) :
      try:
         self._handle = sdb.create_lob()
      except SystemError:
         raise SDBSystemError("Failed to create lob", const.SDB_OOM)

   def __del__(self) :
      if self._handle is not None:
         try:
            rc = sdb.release_lob(self._handle)
            pysequoiadb._raise_if_error("Failed to release lob", rc)
         except SDBBaseError:
            raise
         self._handle = None

   def close(self):
      """close lob

      Exceptions:
         pysequoiadb.error.SDBBaseError
      """
      try:
         rc = sdb.lob_close(self._handle)
         pysequoiadb._raise_if_error("Failed to close lob", rc)
      except SDBBaseError:
         raise

   def get_size(self) :
      """get the size of lob.

      Return Values:
         the size of current lob
      Exceptions:
         pysequoiadb.error.SDBBaseError
      """
      try:
         rc, size = sdb.lob_get_size(self._handle)
         pysequoiadb._raise_if_error("Failed to get size of lob", rc)
      except SDBBaseError:
         raise
      return size

   def get_oid(self):
      """get the oid of lob.

      Return Values:
         the oid of current lob
      Exceptions:
         pysequoiadb.error.SDBBaseError
      """
      try:
         rc, id_str = sdb.lob_get_oid(self._handle)
         pysequoiadb._raise_if_error("Failed to get oid of lob", rc)
      except SDBBaseError :
         raise

      oid = bson.ObjectId(id_str)
      return oid

   def get_create_time(self):
      """get create-time of lob

      Return Values:
         a long int of time
      Exceptions:
         pysequoiadb.error.SDBBaseError
      """
      try:
         rc, mms = sdb.lob_get_create_time(self._handle) ;
         pysequoiadb._raise_if_error("Failed to get createtime of lob", rc)
      except SDBBaseError:
         raise

      return mms

   def seek(self, seek_pos, whence = 0) :
      """seek in lob.

      Parameters:
         Name        Type           Info:
         seek_pos    int            The length to seek
         whence      int            whence of seek, it must be 0/1/2
                                          0 means seek from begin to end of lob
                                          1 means seek from currend position to end of lob
                                          2 means seek from end to begin of lob
      Exceptions:
         pysequoiadb.error.SDBTypeError
         pysequoiadb.error.SDBBaseError
      """
      if not isinstance(seek_pos, int):
         raise SDBTypeError("seek_pos must be an instance of int")
      if not isinstance(whence, int):
         raise SDBTypeError("seek_pos must be an instance of int")
      if whence not in (0, 1, 2):
         raise InvalidParameter("value of whence is in valid",
                                const.SDB_INVALIDARG)
      try:
         rc = sdb.lob_seek(self._handle, seek_pos, whence)
         pysequoiadb._raise_if_error("Failed to seek lob", rc)
      except SDBBaseError:
         raise

   def read(self, len):
      """ream data from lob.

      Parameters:
         Name     Type                 Info:
         len      int                  The length of data to be read
      Return Values:
         binary data of read
      Exceptions:
         pysequoiadb.error.SDBTypeError
         pysequoiadb.error.SDBBaseError
      """
      if not isinstance(len, int):
         raise SDBTypeError("len must be an instance of int")
      try:
         rc, data, size = sdb.lob_read(self._handle, len)
         pysequoiadb._raise_if_error("Failed to read data from lob", rc)
      except SDBBaseError:
         raise
      return data

   def write(self, data, len):
      """write data into lob.

      Parameters:
         Name     Type                 Info:
         data     str                  The data to be written
         len      int                  The length of data to be written
      Exceptions:
         pysequoiadb.error.SDBTypeError
         pysequoiadb.error.SDBBaseError
      """
      if not isinstance(data, basestring):
         raise SDBTypeError("data should be byte or string")

      try:
         rc = sdb.lob_write(self._handle, data, len)
         pysequoiadb._raise_if_error("Failed to write data to lob", rc)
      except SDBBaseError:
         raise
         