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

"""Module of replicanode for python driver of SequoiaDB
"""

try:
   import sdb
except ImportError:
   raise Exception("Cannot find extension: sdb")

import pysequoiadb
from pysequoiadb.common import const
from pysequoiadb.error import SDBBaseError

class replicanode(object):
   """Replica Node of SequoiaDB

   All operation need deal with the error code returned first, if it has. 
   Every error code is not SDB_OK(or 0), it means something error has appeared,
   and user should deal with it according the meaning of error code printed.

   @version: execute to get version
             >>> import pysequoiadb
             >>> print pysequoiadb.get_version()

   @notice : The dict of built-in Python is hashed and non-ordered. so the
             element in dict may not the order we make it. we make a dict and
             print it like this:
             ...
             >>> a = {"avg_age":24, "major":"computer science"}
             >>> a
             >>> {'major': 'computer science', 'avg_age': 24}
             ...
             the elements order it is not we make it!!
             therefore, we use bson.SON to make the order-sensitive dict if the
             order is important such as operations in "$sort", "$group",
             "split_by_condition", "aggregate","create_collection"...
             In every scene which the order is important, please make it using
             bson.SON and list. It is a subclass of built-in dict
             and order-sensitive
   """
   def __init__(self, client):
      """constructor of replica node

      Exceptions:
         pysequoiadb.error.SDBBaseError
      """
      self._client = client
      try:
         self._node = sdb.create_node()
      except SystemError:
         raise SDBBaseError("Failed to alloc node", const.SDB_OOM)

   def __del__(self):
      """release replica node

      Exceptions:
         pysequoiadb.error.SDBBaseError
      """
      if self._node is not None:
         try:
            rc = sdb.release_node(self._node)
            pysequoiadb._raise_if_error("Failed to release node", rc)
         except SDBBaseError:
            raise
         self._node = None
      self._client = None

   def __repr__(self):
      return "Replica Node: %s" % self.get_nodename()

   def connect(self):
      """Connect to the current node.
      
      Exceptions:
         pysequoiadb.error.SDBBaseError
      """
      try:
         rc = sdb.nd_connect(self._node, self._client)
         pysequoiadb._raise_if_error("Failed to connect", rc)
      except SDBBaseError:
         raise

   def get_status(self):
      """Get status of the current node
      
      Return values:
         the status of node
      Exceptions:
         pysequoiadb.error.SDBBaseError
      """
      try:
         rc, nodestatus = sdb.nd_get_status(self._node)
         pysequoiadb._raise_if_error("Failed to get node status", rc)
      except SDBBaseError:
          raise

      return nodestatus

   def get_hostname(self):
      """Get host name of the current node.

      Return values:
         the name of host
      Exceptions:
         pysequoiadb.error.SDBBaseError
      """
      try:
         rc, hostname = sdb.nd_get_hostname(self._node)
         pysequoiadb._raise_if_error("Failed to get host name", rc)
      except SDBBaseError:
         hostname = None
         raise

      return hostname

   def get_servicename(self):
      """Get service name of the current node.

      Return values:
         the name of service
      Exceptions:
         pysequoiadb.error.SDBBaseError
      """
      try:
         rc, servicename = sdb.nd_get_servicename(self._node)
         pysequoiadb._raise_if_error("Failed to get service name", rc)
      except SDBBaseError:
         servicename = None
         raise

      return servicename

   def get_nodename(self):
      """Get node name of the current node.

      Return values:
         the name of node
      Exceptions:
         pysequoiadb.error.SDBBaseError
      """
      try:
         rc, nodename = sdb.nd_get_nodename(self._node)
         pysequoiadb._raise_if_error("Failed to get node name", rc)
      except SDBBaseError:
         nodename = None
         raise

      return nodename

   def stop(self):
      """Stop the node.
      
      Exceptions:
         pysequoiadb.error.SDBBaseError
      """
      try:
         rc = sdb.nd_stop(self._node)
         pysequoiadb._raise_if_error("Failed to stop node", rc)
      except SDBBaseError:
         raise

   def start(self):
      """Start the node.
      
      Exceptions:
         pysequoiadb.error.SDBBaseError
      """
      try:
         rc = sdb.nd_start(self._node)
         pysequoiadb._raise_if_error("Filed to start node", rc)
      except SDBBaseError:
         raise
