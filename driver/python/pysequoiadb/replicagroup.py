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

"""Module of replicagroup for python driver of SequoiaDB
"""

try:
   import sdb
except ImportError:
   raise Exception("Cannot find extension: sdb")

import bson
import types
import pysequoiadb
from pysequoiadb.replicanode import replicanode
from pysequoiadb.common import const
from pysequoiadb import common
from pysequoiadb.error import (SDBBaseError, SDBTypeError)

class replicagroup(object):
   """Replica group of SequoiaDB

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
      """constructor of replica group

      Exceptions:
         pysequoiadb.error.SDBBaseError
      """

      self._client = client
      try:
         self._group = sdb.create_group()
      except SystemError:
         raise SDBBaseError("Failed to alloc replica group", const.SDB_OOM)

   def __del__(self):
      """release replica group object

      Exceptions:
         pysequoiadb.error.SDBBaseError
      """
      if self._group is not None:
         try:
            rc = sdb.release_group(self._group)
            pysequoiadb._raise_if_error("Failed to release replica group", rc)
         except SDBBaseError:
            raise
         self._group = None

      self._client = None

   def __repr__(self):
      try:
         detail = self.get_detail()
         name = detail['GroupName']
         id   = detail['GroupID']
         return "Replica Group: %s, ID:%d" % (name, id)
      except:
         pass

   def get_nodenum(self, nodestatus):
      """Get the count of node with given status in current replica group.
      
      Parameters:
         Name         Type     Info:
         nodestatus   int      The specified status, see Info as below.
      Return values:
         the count of node
      Exceptions:
         pysequoiadb.error.SDBTypeError
         pysequoiadb.error.SDBBaseError
      Info:
         flags : 0 or 1. 
             0 : count of all node
             1 : count of actived node
             2 : count of inactived node
             3 : count of unknown node
      """
      if not isinstance(nodestatus, int):
         raise SDBTypeError("nodestatus be an instance of int")

      if nodestatus not in common.NODE_STATUS.available_options() :
         raise SDBTypeError("nodestatus invalid")

      try:
         rc, nodenum = sdb.gp_get_nodenum(self._group, nodestatus)
         pysequoiadb._raise_if_error("Failed to get count of node", rc)
      except SDBBaseError:
          nodenum = 0
          raise

      return nodenum

   def get_detail(self):
      """Get the detail of the replica group.

      Return values:
         a dict object of query
      Exceptions:
         pysequoiadb.error.SDBBaseError
      """
      try:
         rc, bson_string = sdb.gp_get_detail(self._group)
         pysequoiadb._raise_if_error("Failed to get detail", rc)
      except SDBBaseError:
         detail=None
         raise

      detail, size = bson._bson_to_dict(bson_string, dict, False,
                                        bson.OLD_UUID_SUBTYPE, True)
      return detail

   def get_master(self):
      """Get the master node of the current replica group.

      Return values:
         a replicanode object of query
      Exceptions:
         pysequoiadb.error.SDBBaseError
      """
      try:
         node = replicanode(self._client)
         rc = sdb.gp_get_master(self._group, node._node)
         pysequoiadb._raise_if_error("Failed to get master", rc)
      except SDBBaseError:
         del node
         node = None
         raise

      return node

   def get_slave(self):
      """Get one of slave node of the current replica group, if no slave exists
         then get master.

      Return values:
         a replicanode object of query
      Exceptions:
         pysequoiadb.error.SDBBaseError
      """
      try:
         node = replicanode(self._client)
         rc = sdb.gp_get_slave(self._group, node._node)
         pysequoiadb._raise_if_error("Failed to get slave", rc)
      except SDBBaseError:
         del node
         node = None
         raise

      return node

   def get_nodebyendpoint(self, hostname, servicename):
      """Get specified node from current replica group.
      
      Parameters:
         Name         Type     Info:
         hostname     str      The host name of the node.
         servicename  str      The service name of the node.
      Return values:
         a replicanode object of query
      Exceptions:
         pysequoiadb.error.SDBTypeError
         pysequoiadb.error.SDBBaseError
      """
      if not isintance(hostname, basestring):
         raise SDBTypeError("hostname must be an instance of basestring")
      if not isintance(servicename, basestring):
         raise SDBTypeError("servicename must be an instance of basestring")

      try:
         node = replicanode(self._client)
         rc = sdb.gp_get_nodebyendpoint(self._group, node._node,
                                                 hostname, servicename)
         pysequoiadb._raise_if_error("Failed to get node", rc)
      except SDBBaseError:
         del node
         node = None
         raise

      return ret, node

   def get_nodebyname(self,nodename):
      """Get specified node from current replica group.
      
      Parameters:
         Name         Type     Info:
         nodename     str      The host name of the node.
      Return values:
         a replicanode object of query
      Exceptions:
         pysequoiadb.error.SDBTypeError
         pysequoiadb.error.SDBBaseError
      """
      if not isintance(nodename, basestring):
         raise SDBTypeError("nodename must be an instance of basestring")

      try:
         node = replicanode(self._client)
         rc = sdb.gp_get_nodebyname(self._group, node._node, nodename)
         pysequoiadb._raise_if_error("Failed to get node", rc)
      except SDBBaseError:
         del node
         node = None
         raise

      return node

   def create_node(self, hostname, servicename, dbpath, config = None):
      """Create node in a given replica group.

      Parameters:
         Name         Type     Info:
         hostname     str      The host name for the node.
         servicename  str      The servicename for the node.
         dbpath       str      The database path for the node.
         config       dict     The configurations for the node.
      Exceptions:
         pysequoiadb.error.SDBTypeError
         pysequoiadb.error.SDBBaseError
      """
      if not isinstance(hostname, basestring):
         raise SDBTypeError("host must be an instance of basestring")
      if not isinstance(servicename, basestring):
         raise SDBTypeError("service name must be an instance of basestring")
      if not isinstance(dbpath, basestring):
         raise SDBTypeError("path must be an instance of basestring")
      if config is not None and not isinstance(config, dict):
         raise SDBTypeError("config must be an instance of dict")

      if config is None:
         config = {}

      try:
         rc = sdb.gp_create_node(self._group, hostname, servicename,
                                          dbpath, config)
         pysequoiadb._raise_if_error("Failed to create node", rc)
      except SDBBaseError:
         raise

   def remove_node(self, hostname, servicename, config = None):
      """Remove node in a given replica group.
      
      Parameters:
         Name         Type     Info:
         hostname     str      The host name for the node.
         servicename  str      The servicename for the node.
         config       dict     The configurations for the node.
      Exceptions:
         pysequoiadb.error.SDBTypeError
         pysequoiadb.error.SDBBaseError
      """
      if not isinstance(hostname, basestring):
         raise SDBTypeError("host must be an instance of basestring")
      if not isinstance(servicename, basestring):
         raise SDBTypeError("service name must be an instance of basestring")
      if config is not None and not isinstance(config, dict):
         raise SDBTypeError("config must be an instance of dict")

      try:
         if config is not None:
            bson_config = bson.BSON.encode(config)
            rc = sdb.gp_remove_node(self._group, hostname,
                                             servicename, bson_config)
         else:
            rc = sdb.gp_remove_node(self._group, hostname, servicename)
         pysequoiadb._raise_if_error("Failed to remove node", rc)
      except SDBBaseError:
         raise

   def start(self):
      """Start up current replica group.
      
      Exceptions:
         pysequoiadb.error.SDBBaseError
      """
      try:
         rc = sdb.gp_start(self._group)
         pysequoiadb._raise_if_error("Failed to start", rc)
      except SDBBaseError:
         raise

   def stop(self):
      """Stop current replica group.
      
      Exceptions:
         pysequoiadb.error.SDBBaseError
      """
      try:
         rc = sdb.gp_stop(self._group)
         pysequoiadb._raise_if_error("Failed to stop", rc)
      except SDBBaseError:
         raise

   def is_catalog(self):
      """Test whether current replica group is catalog replica group.
      
      Return values:
         bool
      Exceptions:
         pysequoiadb.error.SDBBaseError
      """
      try:
         rc = sdb.gp_is_catalog(self._group)
         if (const.SDB_OK == rc):
            iscatalog = False
         elif (const.TRUE == rc):
            rc = const.SDB_OK
            iscatalog = True
         else:
            iscatalog = False

         pysequoiadb._raise_if_error("Failed to get catalog info", rc)
      except SDBBaseError:
         raise

      return iscatalog
