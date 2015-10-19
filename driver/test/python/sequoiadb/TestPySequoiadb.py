import time
import bson
import pdb
from bson.son import SON
import pysequoiadb
from pysequoiadb import client
from pysequoiadb import collectionspace
from pysequoiadb import collection
from pysequoiadb import cursor
from pysequoiadb import const
import sys,getopt

from pysequoiadb.error import (SDBBaseError)
from pysequoiadb.error import (SDBTypeError)
from pysequoiadb.error import (SDBEndOfCursor)

#initailize all the global object

"""methods mapping
"""
methods =  ('test_connection',
            'test_user',
            'test_transaction',
            'test_cs_cl',
            'test_list',
            'test_snapshot',
            'test_procedure',
            'test_backup',
            'test_partition',
            'test_index',
            'test_query_sort',
            'test_aggregate',
            'test_cursor',
            'test_replgroup'
            'test_lob'
           )

methods_len = len(methods)

"""static parameters
"""
#change by the real envirment
hostname=None
service=None
data_port=None
"""
hostname='sdbserver3'
service='11810'
data_port='11820'
"""

#cs,cl name
cs_name='pytest_cs'
cl_name='pytest_cl'
cs_global='wx_cs'
cl_global='wx_cl'
range_name = 'range_cl'
hash_name = 'hash_cl'
percent_name = 'percent_cl'

#create cl options
range_option = {'ShardingKey':{'age':1},'ShardingType':'range','ReplSize':0,'Compressed':True}
hash_option = {'ShardingKey':{'age':1},'ShardingType':'hash','Partition':8,'ReplSize':0,'Compressed':True}
percent_option = {'ShardingKey':{'age':1},'ShardingType':'range','ReplSize':0,'Compressed':True}

#global object
sdb = None
cs = None
cl = None
#rc,cs = sdb.get_collection_space(cs_global)
#rc,cl = cs.get_collection(cl_global)
range_cl = None
hash_cl = None
percent_cl = None
option = {'ReplSize':0}

#parase the args
def parse_option():
   if len(sys.argv) < 2:
      usage()
      sys.exit()
   opts,args = getopt.getopt(sys.argv[1:],'hH:p:d:')
   global hostname,service,data_port
   for op,value in opts:
      if op == '-H':
         hostname = value
      elif op == '-p':
         service = int(value)
      elif op == '-d':
         data_port = int(value)
      elif op == '-h':
         usage()
         sys.exit()

#help function
def usage():
   print 'Command options:'
   print '-h         help'
   print '-H   arg   hostname'
   print '-p   arg   coord_port'
   print '-d   arg   data_port'

#initialize all test data
def initialize():
   #global object
   global sdb,cs,cl,range_cl,hash_cl,percent_cl,option
   try:
      sdb = client(hostname,service)
      #print 'connection'
      cs = sdb.create_collection_space(cs_global,0)
      cl = cs.create_collection(cl_global,option)
      #rc,cs = sdb.get_collection_space(cs_global)
      #rc,cl = cs.get_collection(cl_global)
      range_cl = cs.create_collection(range_name,range_option)
      hash_cl = cs.create_collection(hash_name,hash_option)
      percent_cl = cs.create_collection(percent_name,percent_option)
   except Exception as e:
      print 'Error' + e



#clean all test data
def finality():
   global sdb
   try:
      #print 'hello'
      sdb.drop_collection_space(cs_global)
      #print 'world'
      time.sleep(2)
      sdb.disconnect()
   except Exception as e:
      print 'Error' + e

#test PySequoidb class
class TestPySequoiadb(object):


   """create connection,check the connection,create cs and create cl
   insert data,update data,search data and delete data,drop cl and
   drop cs,close connectiontion,check the connection again.
   """
   def test_connection(self):
      sdb = client(hostname,service)
      cs = None
      if not sdb.is_valid():
         print 'create connection failed!'
         return


      hosts = [{'host':'192.168.10.30','service':11200,},
               {'host':'192.168.10.30','service':11200,},
               {'host':hostname,'service':service,},]

      sdb.connect_to_hosts(hosts,user="",password="",policy="random")


      sdb.connect(hostname,service,user="",password="")


      try:
         cs = sdb.create_collection_space(cs_name,0)
         test_cs = sdb.get_collection_space(cs_name)
         if(0!=cmp(cs.get_collection_space_name(),test_cs.get_collection_space_name())):
            print 'get_collection_space failed!'
            return
         cl = cs.create_collection(cl_name)
         test_cl = cs.get_collection(cl_name)
         if(0 !=cmp(cl.get_collection_name(),test_cl.get_collection_name())):
            print 'get_collection failed!'
            return

         #insert data
         data1 = {'age':1,'name':'tom'}
         cl.insert(data1)
         time.sleep(0.5)

         insert_sql = 'insert into '+cs_name+'.'+cl_name+'(age,name) values(24,\'kate\')'
         sdb.exec_update(insert_sql)
         time.sleep(0.5)

         #update data
         cs = sdb.get_collection_space(cs_name)
         cl = cs.get_collection(cl_name)

         update_sql = 'update '+cs_name+'.'+cl_name+' set name = \'tom_new\' where age = 1'
         sdb.exec_update(update_sql)
         rule = {'$set':{'name':'kate_new'}}
         cond = {'age':{'$et':24}}
         cl.update(rule,condition=cond)
         time.sleep(0.5)


         #search data
         cursor = cl.query(condition=cond)
         #rc,record = cursor.next()
         while True:
            try:
               record = cursor.next()
               if (0!=cmp(record['name'],'kate_new')):
                  print 'search data failed1!'
                  return
            except SDBEndOfCursor :
               break
            except SDBBaseError :
               raise

         select_sql = 'select name from '+cs_name+'.'+cl_name+' where age=1'
         cursor = sdb.exec_sql(select_sql)
         #rc,record = cursor.next()
         while True:
            try:
               record = cursor.next()
               if (0!=cmp(record['name'],'tom_new')):
                  print 'search data failed2!'
                  return

            except SDBEndOfCursor :
               break
            except SDBBaseError :
               raise


          #delete data
         delete_sql = 'delete from '+cs_name+'.'+cl_name +' where age=1'
         sdb.exec_update(delete_sql)
         cl.delete(condition=cond)
         time.sleep(0.5)

         count = cl.get_count()
         if 0 != count:
            print 'delete data failed!'
            return

      except Exception  as e:
         print e
      finally:
         sdb.drop_collection_space(cs_name)
         time.sleep(0.5)
         if sdb.is_valid():
            sdb.disconnect()
         time.sleep(1)
         if sdb.is_valid():
            print 'disconnect is failed!'


   """create user and use the new user login then delete the user
   """
   def test_user(self):
      username = 'test'
      password = 'test'
      sdb = client(hostname,service)

      #deal with the SDBTypeError

      try:
         sdb.create_user(119,password)
      except SDBTypeError as e:
         print 'catch the SDBTpyeError'

      try:

         #create new user
         sdb.create_user(username,password)

         #use new user login
         sdb_new = client(hostname,service,username,password)
         if not sdb_new.is_valid():
            print 'login failed by using new user!'
            return
      except Exception as e:
         print  e
      finally:
         sdb.remove_user(username,password)
         sdb_new.disconnect()
         sdb.disconnect()


   """create transaction and then insert,update,delete data,commit transaction,or rollback
   """
   def test_transaction(self):
      try:
         """
         rc,cl = cs.get_collection(cl_name)
         option = {'ReplSize':2}
         if rc != const.SDB_OK:
            rc = const.SDB_OK
            rc,cl = cs.create_collection(cl_name,option)
            time.sleep(1)
         """
         data1 = {'name':'mike','score':98}

         #insert transaction
         sdb.transaction_begin()
         cl.insert(data1)
         sdb.transaction_commit()
         time.sleep(0.1)
         count = cl.get_count()
         #print count.__str__()
         if 1 != count:
            print 'insert data failed!'
            return

         #update transaction
         rule = {'$set':{'score':60}}
         cond = {'name':{'$et':'mike'}}
         sdb.transaction_begin()
         cl.update(rule,condition = cond)
         sdb.transaction_commit()
         time.sleep(0.1)
         cursor = cl.query(condition=cond)
         #rc,record = cursor.next()
         while True :
            try:
               record = cursor.next()
               if 60 != record['score']:
                  print 'update data failed!'
                  return
            except SDBEndOfCursor :
                  break
            except SDBBaseError :
                  raise


         #delete transaction rollback
         sdb.transaction_begin()
         cl.delete(condition=cond)
         sdb.transaction_rollback()
         time.sleep(0.1)
         count = cl.get_count()
         if 1!= count:
            print 'rollback failed!'
            return

         #delete transaction
         sdb.transaction_begin()
         cl.delete(confition=cond)
         sdb.transaction_commit()
         time.sleep(0.1)
         count = cl.get_count()
         if 0 != count:
            print 'delete data failed!'
            return

      except SDBBaseError as e:
         print e.code
         print e.detail
      finally:
         cl.delete()

   """create repeat name cs ,cs name include '$',cs name's length greater than
      128,get one  not exist cs,cl,delete one not exist cs,cl,list all cs and cl
   """
   def test_cs_cl(self):
      try:
         #create a repeat name cs
         try:
            test_cs = sdb.create_collection_space(cs_global)
         except Exception as e:
            print 'create repeat name cs'

         #create cs name include '$'
         cs_name = '$hello'
         try:
            test_cs = sdb.create_collection_space(cs_name)
         except Exception as e:
            print 'cs name include "$" '

         #create cs name's length > 128
         cs_name ='abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
         try:
            test_cs = sdb.create_collection_space(cs_name)
         except Exception as e:
            print "cs name's length greater than 128"

         #get one not exist cs
         try:
            test_cs = sdb.get_collection_space('not_exist_cs')
         except Exception as e:
            print 'get a not exist cs'

         #get one not exist cl
         try:
            test_cl = cs.get_collection('not_exist_cl')
         except Exception as e:
            print 'get a not exist cl'

         #list all cs
         cursor = sdb.list_collection_spaces()
         #rc,record = cursor.next()
         while True:
            try:
               record = cursor.next()
               print record['Name']
            except SDBEndOfCursor :
               break
            except SDBBaseError :
               raise

         #list all cl
         cursor = sdb.list_collections()
         while True:
            try:
               record = cursor.next()
               print record['Name']
            except SDBEndOfCursor :
               break
            except SDBBaseError :
               raise

      except SDBBaseError as e:
         print e.code
         print e.detail
      finally:
         cl.delete()

   """test get_list
   """
   def test_list(self):
      try:
         try:
            cursor = sdb.get_list(0)
         except Exception as e:
            print 'list (0) error'
         temp = {'$gt':1}
         query = {'SessionID':temp}
         select = {'Contexts':'aa'}
         order = {'SessionID':1}
         #list by options
         cursor = sdb.get_list(0,condition=query,selector=select,order_by=order)
         #rc,record = cursor.next()
         while True:
            try:
               record = cursor.next()
               print record['Contexts']
            except SDBEndOfCursor :
               break
            except SDBBaseError :
               raise

         #list has no options
         try:
            cursor = sdb.get_list(4)
         except Exception as e:
            print 'list has no options error'
         #rc,record = cursor.next()
         while True :
            try :
               record = cursor.next()
               print record['Name']
            except SDBEndOfCursor :
               break
            except SDBBaseError :
               raise
      except Exception as e:
         print  e
      finally:
         cl.delete()

   """test get_snapshot
   """
   def test_snapshot(self):
      try:
         try:
            cursor = sdb.get_snapshot(0)
         except Exception as e:
            print 'get snapshot(0) error'
         query = {'SessionID':{'$ne':1}}
         select = {'SessionID':1}
         order = {'SessionID':1}
         #snapshot by options 
         cursor = sdb.get_snapshot(0,condition=query,selector=select,order_by=order)
         #rc,record = cursor.next()
         while True:
            try:
               record = cursor.next()
               print record
            except SDBEndOfCursor :
               break
            except SDBBaseError :
               raise


         #snapshot has no options
         cursor = sdb.get_snapshot(5)
         rc = const.SDB_OK
         while True:
            try:
               record = cursor.next()
               print record['PageSize']
            except SDBEndOfCursor :
               break
            except SDBBaseError :
               raise
      except Exception as e:
         print  e
      finally:
         cl.delete()

   """test create normal and  abnormal procedure,list procedure,exec normal and
      abnormal procedure,delete normal and abnormal procedure
   """
   def test_procedure(self):
      try:

         #create normal procedure
         code = 'function sum(x,y){return x+y;}'
         sdb.create_procedure(code)

         #create abnormal procedure
         code = 'func sum(x,y){return x+z;}'
         try:
            sdb.create_procedure(code)
         except Exception as e:
            print 'create a error procedure'

         #create repeat name procedure
         code = 'function sum(x,y){return x+y;}'
         try:
            sdb.create_procedure(code)
         except Exception as e:
            print 'create repeat procedure'

         #list procedure
         condition = {'name':'sum'}
         cursor = sdb.list_procedures(condition=condition)
         flag = False
         #rc,record = cursor.next()
         while True:
            try:
               record = cursor.next()
               print record['name']
               if ( 0 == cmp(record['name'],'sum')):
                  flag = True
                  break
            except SDBEndOfCursor :
               break
            except SDBBaseError :
               raise

         if flag == False:
            print 'list procedure failed!'


         #exec normal procedure
         result = sdb.eval_procedure('sum(1,2)')
         #rc,record = result.next()
         while True:
            #print record['value']
            try:
               record = result.next()
               if record['value'] != 3:
                  print 'eval procedure failed!'
            except SDBEndOfCursor:
               break
            except SDBBaseError:
               raise
            #break

         #print 'world'

         #exec abnormal procedure
         try:
            result = sdb.eval_procedure('no_exist(1,2)')
         except Exception as e:
            print 'eval not exist procedure '

         #del normal procedure
         sdb.remove_procedure('sum')

         #del abnormal procedure
         try:
            sdb.remove_procedure('not_exist')
         except Exception as e:
            print 'remove not exist procedure'

      except SDBBaseError as e:
         print e.code
         print e.detail
      finally:
         cl.delete()

   """test create backup,list backup,delete bakup
   """
   def test_backup(self):
      try:
         #create backup
         option = {'GroupName':'datagroup','Name':'datagroup_backup','OverWrite':True}
         try:
            sdb.backup_offline(option)
         except Exception as e:
            print 'backup_offline error'

         #list backup
         option = {'GroupName':'datagroup'}
         condition = {'NodeName':{'$ne':'aa'}}
         selector = {'Name':1}
         cursor = sdb.list_backup(option,condition=condition,selector=selector)
         #rc,record = cursor.next()
         while True:
            try:
               record = cursor.next()
               if(0 != cmp(record['Name'],'datagroup_backup')):
                  print 'list backup failed!'
                  break
            except SDBEndOfCursor :
               break
            except SDBBaseError:
               raise

         #delete backup
         option = {'Name':'datagroup_backup'}
         try:
            sdb.remove_backup(option)
         except Exception as e:
            print 'remove backup error'

      except Exception as e:
         print  e
      finally:
         cl.delete()

   """test create range partition and hash pratition
   """
   def test_partition(self):
      try:
         #range partition
         cursor = sdb.get_snapshot(4,condition={'Name':{'$et':cs_global+'.'+range_name}})
         #rc,record = cursor.next()
         while True:
            try:
               record = cursor.next()
               if( 0 == cmp( record['Details'][0]['GroupName'],'datagroup')):
                  src = 'datagroup'
                  dst = 'datagroup1'
                  break
               else:
                  src = 'datagroup1'
                  dst = 'datagroup'
                  break
            #rc,record = cursor.next()
            except SDBEndOfCursor :
               break
            except SDBBaseError :
               raise
         print 'src='+src+',dst='+dst

         for i in range(0,100):
            range_cl.insert({'age':i,'name':'mike'+str(i)})
         range_cl.split_by_condition(src,dst,{'age':10},{'age':60})
         time.sleep(3)

         temp_sdb = client(hostname,data_port)
         temp_cs = temp_sdb.get_collection_space(cs_global)
         temp_cl = temp_cs.get_collection(range_name)
         count = temp_cl.get_count()
         print 'count='+count.__str__()
         if count != 50:
            print 'range partion failed!'

         #hash partition
         cursor = sdb.get_snapshot(4,condition={'Name':{'$et':cs_global+'.'+hash_name}})
         #rc,record = cursor.next()
         while True:
            try:
               record = cursor.next()
               if( 0 == cmp( record['Details'][0]['GroupName'],'datagroup')):
                  src = 'datagroup'
                  dst = 'datagroup1'
                  break
               else:
                  src = 'datagroup1'
                  dst = 'datagroup'
                  break
            except SDBEndOfCursor :
               break
            except SDBBaseError :
               raise
         print 'src='+src+',dst='+dst
         for i in range(0,100):
            hash_cl.insert({'age':i,'name':'mike'+str(i)})
         hash_cl.split_by_condition(src,dst,{'Partition':4},{'Partition':8})
         temp_cl = temp_cs.get_collection(hash_name)
         count = temp_cl.get_count()
         print 'count='+count.__str__()
         if count != 57 and count != 43:
            print 'hash partion failed!'

         #precent
         cursor = sdb.get_snapshot(4,condition={'Name':{'$et':cs_global+'.'+percent_name}})
         #rc,record = cursor.next()
         while True:
            try:
               record = cursor.next()
               if( 0 == cmp( record['Details'][0]['GroupName'],'datagroup')):
                  src = 'datagroup'
                  dst = 'datagroup1'
                  break
               else:
                  src = 'datagroup1'
                  dst = 'datagroup'
                  break
            except SDBEndOfCursor :
                  break
            except SDBBaseError :
                  raise
         print 'src='+src+',dst='+dst
         for i in range(0,100):
            percent_cl.insert({'age':i,'name':'mike'+str(i)})
         percent_cl.split_by_percent(src,dst,50.0)

         temp_cl = temp_cs.get_collection(percent_name)
         count = temp_cl.get_count()
         print 'count='+count.__str__()
         if count != 50:
            print 'percent partion failed!'

      except Exception as e:
         print  e
      finally:
         range_cl.delete()
         hash_cl.delete()
         percent_cl.delete()

   """test query and sort
   """
   def test_query_sort(self):
      try:
         for i in range(1,100):
            cl.insert({'age':i,'name':'mike'+str(i)})

         gt = {'$gt':96}
         condition = {'age':gt}
         selector = {'age':'','name':''}
         order = {'age':-1}
         cursor = cl.query(condition=condition,selector=selector,order_by=order)
         while True:
            try:
               record = cursor.next()
               print record['age']
               if record['age'] != 99:
                  print 'query sort error!'
               break
            except SDBEndOfCursor :
               break
            except SDBBaseError :
               raise

      except Exception as e:
         print  e
      finally:
         cl.delete()

   """test create index,list index,remove index
   """
   def test_index(self):
      try:
         #create index
         index_def = {'name':1,'age':-1}
         idx_name = 'nameIndex'
         is_unique = True
         is_enforce = True
         try:
            cl.create_index(index_def,idx_name,is_unique,is_enforce)
         except Exception as e:
            print 'create index error'

         idx_name = 'nameIndex'
         #get index name
         cursor = cl.get_indexes(idx_name)
         while True:
            try:
               record = cursor.next()
               if(0 != cmp( record['IndexDef']['name'],idx_name)):
                  print 'get index failed!'
            except SDBEndOfCursor :
                  break
            except SDBBaseError :
                  raise

         #get all index name
         flag  = False
         idx_name = ''
         cursor = cl.get_indexes(idx_name)
         #rc,record = cursor.next()
         while True:
            try:
               record = cursor.next()
               if (0 == cmp(record['IndexDef']['name'],'nameIndex')):
                  flag = True
                  break
            except SDBEndOfCursor:
               break
            except SDBBaseError:
               raise

         if flag == False:
            print 'get all index failed!'

         #remove index
         idx_name = 'nameIndex'
         try:
            cl.drop_index(idx_name)
         except Exception as e:
            print 'drop index error'

      except Exception as e:
         print e
      finally:
         cl.delete()

   """test cl aggergate
   """
   def test_aggregate(self):
      try:
         data1 = {'no':1000,'score':80,'interest':['basketball','football'],'major':'computer th','dep':'computer','info':{'name':'tom','age':25,'gender':'man'}}
         data2 = {'no':1001,'score':90,'interest':['basketball','football'],'major':'computer sc','dep':'computer','info':{'name':'mike','age':24,'gender':'lady'}}
         data3 = {'no':1002,'score':85,'interest':['basketball','football'],'major':'computer en','dep':'computer','info':{'name':'kkk','age':25,'gender':'man'}}
         data4 = {'no':1003,'score':92,'interest':['basketball','football'],'major':'computer en','dep':'computer','info':{'name':'mmm','age':25,'gender':'man'}}
         data5 = {'no':1004,'score':88,'interest':['basketball','football'],'major':'computer sc','dep':'computer','info':{'name':'ttt','age':25,'gender':'man'}}

         cl.insert(data1)
         cl.insert(data2)
         cl.insert(data3)
         cl.insert(data4)
         cl.insert(data5)
         match = SON({'$match':{'interest':{'$exists':1}}})
         group = SON({'$group':{'_id':'$major','avg_age':{'$avg':'$info.age'},'major':{'$first':'$major'}}})
         sort = SON({'$sort':{'avg_age':1, 'major':-1}})
         skip = {'$skip':0}
         limit = {'$limit':5}

         option = [match,group,sort,skip,limit]
         cursor = cl.aggregate(option)
         #rc,record = cursor.next()
         while True:
            try:
               record = cursor.next()
               print record
            except SDBEndOfCursor :
               break
            except SDBBaseError :
               raise


      except Exception as e:
         print e
      finally:
         cl.delete()

   """test two sessions close all cursor
   """
   def test_cursor(self):
      try:
         test_sdb1 = client(hostname,service)
         test_sdb2 = client(hostname,service)

         cs.create_collection('hello')
         cs.create_collection('world')

         try:
            cursor1 = test_sdb1.list_collections()
         except Exception as e:
            print 'list collections error'

         try:
            cursor2 = test_sdb2.list_collections()
         except Exception as e:
            print 'sdb2 list collections error'

         try:
            test_sdb1.close_all_cursors()
            record = cursor1.next()
         except Exception as e:
               print 'the cursor is closed ,so that can not to use the cursor.'

         record = cursor2.next()

      except Exception as e:
         print e
      finally:
         cl.delete()

   """test create replgroup ,add node ,remove node,list replgroups,remove
      replgroups
   """
   def test_replgroup(self):
      try:
         #create replgroup
         try:
            rg = sdb.create_replica_group('test_group')
         except SDBBaseError as e:
            print 'create replicate group error'
            print e.code
            print e.detail

         #get replgroup by name
         try:
            rg = sdb.get_replica_group_by_name('test_group')
         except SDBBaseError as e:
            print e.code
            print e.detail
            print 'get replica group by name error'

         query = {'GroupName':{'$et':'test_group'}}
         select = {'GroupID':1}
         order = {'GroupID':1}
         rg = sdb.get_list(7,condition=query,selector=select)
         #rc,record = rg.next()
         while True:
            try:
               record = rg.next()
               groupid = record['GroupID']
            except SDBEndOfCursor :
               break
            except SDBBaseError:
               raise

         #list replgroup
         cursor = sdb.list_replica_groups()
         #rc,record = cursor.next()
         while True:
            try:
               record = cursor.next()
               print record['GroupID']
            except SDBEndOfCursor :
               break
            except SDBBaseError :
               raise

         #get replgroup by id
         try:
            print 'groupid='+str(groupid)
            rg = sdb.get_replica_group_by_id(groupid)
         except SDBBaseError as e:
            print e.code
            print e.detail
            print 'get replgroup by id  error'

         #create node
         try:
            rg.create_node(hostname,'18000','/opt/trunk/database/data/18000')
            print 'create 18000 end'
            rg.create_node(hostname,'19000','/opt/trunk/database/data/19000')
            print 'create 19000 end'
         except SDBBaseError as e:
            print e.code
            print e.detail
            print 'create node error'

         #start group
         try:
            rg.start()
            #print 'aa'
         except Exception as e:
            print 'start group error'


         #time.sleep(10)

         #stop group
         try:
            rg.stop()
         except SDBBaseError as e:
            print e.code
            print e.detail
            print 'stop group error'


         #remove node
         try:
            rg.remove_node(hostname,'18000')
         except Exception as e:
            print 'remove node error.'

         #remove replgroup
         try:
            sdb.remove_replica_group('test_group')
         except Exception as e:
            print 'remove replica group error'

      except Exception as e:
         print  e
      finally:
         cl.delete()

   def test_lob(self):
      try:
         #create a new lob,oid = None
         try:
            obj = cl.create_lob()
            obj.write("hello",5)
            new_oid = obj.get_oid()
            #print new_oid
            obj.close()
         except SDBBaseError :
            print 'create oid = None filed'

         #create a new lob,oid = 54471e5161a3be8176000000
         #write data
         try:
            obj = cl.create_lob( bson.ObjectId("5448a5181c3eb9e00b000001" ))
            data = "1234567891011121314151617181920"
            obj.write( data,31 )
            obj.close()

         except SDBBaseError :
            print 'create oid = 54471e5161a3be8176000000 failed'

         #create a repeat name lob
         try:
            obj = cl.create_lob( bson.ObjectId("5448a5181c3eb9e00b000001" ) )
         except SDBBaseError :
            print 'create a repeat name lob'


         #list lobs

         i = 0 ;
         cr = cl.list_lobs()
         while True:
            try:
               lob = cr.next()
               i = i+ 1
            except SDBEndOfCursor :
               break
            except SDBBaseError :
               raise

         if ( i != 2 ):
            print 'list lobs failed'



         #seek and read data from a lob
         obj = cl.get_lob(bson.ObjectId("5448a5181c3eb9e00b000001" ) )
         oid = obj.get_oid()
         #print oid
         obj.seek( 11,0)
         data = obj.read(20)
         obj.close()
         if( 0 != cmp(data,"11121314151617181920")):
            print 'seek and read data from a lob failed'


         #get a exist lob
         obj  = cl.get_lob( bson.ObjectId("5448a5181c3eb9e00b000001" ) )
         oid = obj.get_oid()
         if ( 0 != cmp(oid, bson.ObjectId("5448a5181c3eb9e00b000001" ) )):
            print 'get a exist lob failed'

         #get a not exist lob
         try:
            obj = cl.get_lob( bson.ObjectId("54471e5161a3be8176000002" )  )
            print 'get a not exist lob failed'

         except SDBBaseError:
            print 'get a not exist lob'

         #get lob's size
         if ( 31 != obj.get_size()):
            print "get lob's size failed"


         #get lob's create time
         pysequoiadb._print(obj.get_create_time())


         #remove a exist lob
         try:
            cl.remove_lob(obj.get_oid())
            cl.remove_lob(new_oid)
         except SDBBaseError:
            print 'remove a exist log failed'

         obj.close()

         #remove a not exist lob
         try:
            cl.remove_lob(bson.ObjectId("54471e5161a3be8176000002" ))
         except SDBBaseError :
            print 'remove a not exist lob'



      except Exception as e:
         print e
      finally:
         cl.delete()

if __name__ == "__main__":
   try:
      parse_option()
      initialize()
      test = TestPySequoiadb()
      for i in range(0,methods_len):
         getattr(test,methods[i])()
      #test.test_lob()
      print 'test success.'
   except Exception as e:
      print e
      print 'test fail'
   finally:
      finality()

