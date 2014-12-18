#!/bin/bash
#***********************************************************************************
#collect all sdbdiag.log/sdb.conf/sdbcmd.log/sdbcm.log/sdbcm.conf file to SDBSNAPS
#***********************************************************************************
function sdbPortGather()
{
   HOST=$1
   DBPATH=$2
   PORT=$3
   installpath=$4

   if [ "$PORT" != "" ] ; then
      mkdir -p SDBNODES/
   else
      return 0
   fi
   #config file
   sdbPortConf
   #log file
   sdbPortLog
   #sdbcm config and log file
   sdbCmConfLog
   #if folder don't have file ,then delete it
   lsfold=`ls SDBNODES/` >>/dev/null 2>&1
   if [ "$lsfold" == "" ] ; then
      rm -rf SDBNODES/
   else
      sdbEchoLog "EVENT" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Success to collect config and log file"
   fi
}

function sdbPortGatherPart()
{
   HOST=$1
   DBPATH=$2
   PORT=$3
   installpath=$4
   sdbconf=$5
   sdblog=$6
   sdbcm=$7

   if [ "$sdbconf" != "false" ] || [ "$sdblog" != "false" ] || [ "$sdbcm" != "false" ] ; then
      mkdir -p SDBNODES/
   else
      return 0
   fi
   #config file
   if [ "$sdbconf" == "true" ] ; then
      sdbPortConf
   fi
   #log file
   if [ "$sdblog" == "true" ] ; then
      sdbPortLog
   fi
   #sdbcm config and log file
   if [ "$sdbcm" == "true" ] ; then
      sdbCmConfLog
   fi

   #if folder don't have file ,then delete it
   lsfold=`ls SDBNODES/` >>/dev/null 2>&1
   if [ "$lsfold" == "" ] ; then
      rm -rf SDBNODES/
      sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Don't have file in SDBNODES/"
   else
      sdbEchoLog "EVENT" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Success to collect config and log file"
   fi

}

function sdbPortConf()
{
   #get sequoiadb config path
   confpath=$installpath/conf/local
   ls $confpath/$PORT >> /dev/null 2>&1
   conf=$?
   if [ $conf -eq 0 ] ; then
      cp $confpath/$PORT/sdb.conf SDBNODES/$HOST.$PORT.sdb.conf >> /dev/null 2>&1
      rc=$?
      if [ $rc -ne 0 ] ; then
         echo "Failed to collect $HOST:$PORT sdb.conf."
         sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Failed to collect $HOST:$confpath/$PORT/sdb.conf"
      fi
   fi
}
function sdbPortLog()
{
   #collect sdbdiag.log file
   core=`find -name "*core*"`
   if [ "$core" != "" ] ; then
      size=`du -s sdbdiag*`
      crsize=`echo $size|cut -d " " -f 1`
      if [ $crsize -lt 2097152 ] ; then
         tar -zcvf $DBPATH.tar.gz $DBPATH/diaglog/ >>/dev/null 2>&1 
         rc=$?
         cp -r $DBPATH.tar.gz SDBNODES/ >>/dev/null 2>&1
         rc1=$?
         if [ $rc -ne 0 ] || [ $rc1 -ne 0 ] ; then
            echo "Failed to collect $HOST:$PORT sdbdiag.log and core file[big]"
            sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Failed to collect $HOST:$PORT sdbdiag.log"
         fi
      else
         tar -zcvf $DBPATH.tar.gz $DBPATH/diaglog/trap* $DBPATH/diaglog/sdbdiag.log >>/dev/null 2>&1
         rc=$? 
         cp -r $DBPATH.tar.gz SDBNODES/ >>/dev/null 2>&1
         rc1=$?
         if [ $rc -ne 0 ] || [ $rc1 -ne 0 ] ; then
            echo "Failed to collect $HOST:$PORT sdbdiag.log"
            sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Failed to collect $HOST:$PORT sdbdiag.log and core file"
         fi
      fi
   else
      cp $DBPATH/diaglog/sdbdiag.log SDBNODES/$HOST.$PORT.diaglog >>/dev/null 2>&1
      rc=$?
      if [ $rc -ne 0 ] ; then
         echo "Failed to collect $HOST:$PORT sdbdiag.log"
         sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Failed to collect $HOST:$DBPATH/diaglog/sdbdiag.log"
      fi
   fi
}

function sdbCmConfLog()
{
   #collect sdbcm log
   cmlogpath=$installpath/conf/log
   cmcfpath=$installpath/conf
   #collect sdbcmd log
   ls SDBNODES/$HOST.sdbcmd.log >>/dev/null 2>&1
   cmdlog=$?
   if [ $cmdlog -ne 0 ] ; then
      cp $cmlogpath/sdbcmd.log SDBNODES/$HOST.sdbcmd.log
      rc=$?
      if [ $rc -ne 0 ] ; then
         echo "Failed to collect $HOST:$PORT sdbcmd.log file"
         sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Failed to collect $HOST:$PORT sdbcmd.log file"
      fi
   fi
   #collect sdbcm log
   ls SDBNODES/$HOST.sdbcm.log >>/dev/null 2>&1
   cmlog=$?
   if [ $cmlog -ne 0 ] ; then
      cp $cmlogpath/sdbcm.log SDBNODES/$HOST.sdbcm.log
      rc=$?
      if [ $rc -ne 0 ] ; then
         echo "Failed to collect $HOST:$PORT sdbcm.log file"
         sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Failed to collect $HOST:$PORT sdbcm.log file"
      fi
   fi
   #collect sdbcm config file
   ls SDBNODES/$HOST.sdbcm.conf >>/dev/null 2>&1
   cmconf=$?
   if [ $cmconf -ne 0 ] ; then
      cp $cmcfpath/sdbcm.conf SDBNODES/$HOST.sdbcm.conf
      rc=$?
      if [ $rc -ne 0 ] ; then
         echo "Failed to collect $HOST:$PORT sdbcm.conf file"
         sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Failed to collect $HOST:$PORT sdbcm.conf file"
      fi
   fi

}

#*********************************************************************
#collect catalog information snapshot 
#*********************************************************************
function sdbSnapShotCataLog()
{
   HOST=$1
   PORT=$2
   installpath=$3

   if [ "$PORT" != "" ] ; then
      mkdir -p SDBSNAPS/
   else
      return 0
   fi
   SDB=$installpath/bin/sdb
   $SDB "var db=new Sdb('localhost',$PORT)" >>sdbsupport.log 2>&1
   rc=$?
   if [ $rc -ne 0 ] ; then
      $SDB "quit"
      echo "Failed to collect snapshot.Please check over the node $PORT is run or not !"
   else
      $SDB "db.snapshot(SDB_SNAP_CATALOG)" >> SDBSNAPS/$HOST.$PORT.snapshot_catalog
      $SDB "db.listReplicaGroups()" >> SDBSNAPS/$HOST.$PORT.listGroups
      $SDB "quit"
   fi

   #if folder don't have file ,then delete it
   lsfold=`ls SDBSNAPS/` >>/dev/null 2>&1
   if [ "$lsfold" == "" ] ; then
      rm -rf SDBSNAPS/
      sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Don't have file in SDBSNAPS/"
   else
      sdbEchoLog "EVENT" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Success to collect sdb snapshot information"
   fi

}
function sdbCoordSnapShot()
{
   HOST=$1
   PORT=$2
   installpath=$3
   catalog=$4
   group=$5

   if [ "$catalog" != "false" ] || [ "$group" != "false" ] ; then
      mkdir -p SDBSNAPS/
   else
      return 0
   fi

   SDB=$installpath/bin/sdb
   $SDB "var db = new Sdb('localhost',$PORT" >>sdbsupport.log 2>&1
   rc=$?
   if [ $rc -ne 0 ] ; then
      $SDB "quit"
      echo "Failed to collect snapshot.Please check over the node $PORT is run or not !"
   else
      if [ "$catalog" == "true" ] ; then
         $SDB "db.snapshot(SDB_SNAP_CATALOG)" >> SDBSNAPS/$HOST.$PORT.snapshot_catalog 2>&1
      fi
      if [ "$group" == "true" ] ; then
         $SDB "db.listReplicaGroups()" >> SDBSNAPS/$HOST.$PORT.listGroups 2>&1
      fi
      $SDB "quit"
   fi

   #if folder don't have file ,then delete it
   lsfold=`ls SDBSNAPS/` >>/dev/null 2>&1
   if [ "$lsfold" == "" ] ; then
      rm -rf SDBSNAPS/
      sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Don't have file in SDBSNAPS/"
   else
      sdbEchoLog "EVENT" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Success to collect sdb snapshot information"
   fi

}


#**********************************************************************
#snapshot of sequoiadb to SDBSNAPS
#**********************************************************************
function sdbSnapShot()
{
   HOST=$1
   PORT=$2
   installpath=$3

   if [ "$PORT" != "" ] ; then
      mkdir -p SDBSNAPS/
   else
      return 0
   fi

   SDB=$installpath/bin/sdb
   $SDB "var db=new Sdb('localhost',$PORT)" >>sdbsupport.log 2>&1
   rc=$?
   if [ $rc -ne 0 ] ; then
      $SDB "quit"
      echo "Failed to collect snapshot.Please check over the node $PORT is run or not !"
   else
      $SDB "db.snapshot(SDB_SNAP_CONTEXTS)" >> SDBSNAPS/$HOST.$PORT.snapshot_contests
      $SDB "db.snapshot(SDB_SNAP_SESSIONS)" >> SDBSNAPS/$HOST.$PORT.snapshot_sessions
      $SDB "db.snapshot(SDB_SNAP_COLLECTIONS)" >> SDBSNAPS/$HOST.$PORT.snapshot_collections
      $SDB "db.snapshot(SDB_SNAP_COLLECTIONSPACES)" >> SDBSNAPS/$HOST.$PORT.snapshot_collectionspace
      $SDB "db.snapshot(SDB_SNAP_DATABASE)" >> SDBSNAPS/$HOST.$PORT.snapshot_database
      $SDB "db.snapshot(SDB_SNAP_SYSTEM)" >> SDBSNAPS/$HOST.$PORT.snapshot_system
      $SDB "quit"
   fi

   #if folder don't have file ,then delete it
   lsfold=`ls SDBSNAPS/` >>/dev/null 2>&1
   if [ "$lsfold" == "" ] ; then
      rm -rf SDBSNAPS/
   sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Don't have file in SDBSNAPS/"
   else
      sdbEchoLog "EVENT" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Success to collect sdb snapshot information"
   fi
}

#sdbSnapShot Single Extract
function sdbSnapShotExtract()
{
   HOST=$1
   PORT=$2

   context=$3
   session=$4
   collection=$5
   collectionspace=$6
   database=$7
   system=$8
   installpath=$9

   if [ "$PORT" != "" ] || [ "$context" == "true" ] || [ "$session" == "true" ] || [ "$collection" == "true" ] || [ "$collectionspace" == "true" ] || [ "$database" == "true" ] || [ "$system" == "true" ] ; then
      mkdir -p SDBSNAPS/
   else
      return 0
   fi

   SDB=$installpath/bin/sdb
   $SDB "var db=new Sdb('localhost',$PORT)" >>sdbsupport.log 2>&1
   if [ $? -ne 0 ] ; then
      $SDB "quit"
      echo "Failed to collect snapshot.Please check over the node $PORT is run or not !"
      sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "the node $PORT is not run"
   else
      if [ "$context" == "true" ] ; then
         $SDB "db.snapshot(SDB_SNAP_CONTEXTS)" >> SDBSNAPS/$HOST.$PORT.snapshot_contests
      fi
      if [ "$session" == "true" ] ; then
         $SDB "db.snapshot(SDB_SNAP_SESSIONS)" >> SDBSNAPS/$HOST.$PORT.snapshot_sessions
      fi
      if [ "$collection" == "true" ] ; then
         $SDB "db.snapshot(SDB_SNAP_COLLECTIONS)" >> SDBSNAPS/$HOST.$PORT.snapshot_collections
      fi
      if [ "$collectionspace" == "true" ] ; then
         $SDB "db.snapshot(SDB_SNAP_COLLECTIONSPACES)" >> SDBSNAPS/$HOST.$PORT.snapshot_collectionspace
      fi
      if [ "$database" == "true" ] ; then
         $SDB "db.snapshot(SDB_SNAP_DATABASE)" >> SDBSNAPS/$HOST.$PORT.snapshot_database
      fi
      if [ "$system" == "true" ] ; then
         $SDB "db.snapshot(SDB_SNAP_SYSTEM)" >> SDBSNAPS/$HOST.$PORT.snapshot_system
      fi
      $SDB "quit"
   fi

   #if folder don't have file ,then delete it
   lsfold=`ls SDBSNAPS/` >>/dev/null 2>&1
   if [ "$lsfold" == "" ] ; then
      rm -rf SDBSNAPS/
      sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Don't have file in SDBSNAPS/"
   else
      sdbEchoLog "EVENT" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Success to collect sdb snapshot information"
   fi
}

#collect all hardware infomation
function sdbHardwareInfoAll()
{
   HOST=$1

   mkdir -p HARDINFO/

   sdbCpu
   sdbMemory
   sdbDisk
   sdbNetcard
   sdbMainboard

   #if folder don't have file ,then delete it
   lsfold=`ls HARDINFO/` >>/dev/null 2>&1
   if [ "$lsfold" == "" ] ; then
      rm -rf HARDINFO/
   else
      sdbEchoLog "EVENT" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Success to collect Hardware Information."
   fi
}
##Hardware : collect cpu information
function sdbCpu()
{
   echo "######>lscpu" >> HARDINFO/$HOST.cpu.info 2>&1 
   lscpu >> HARDINFO/$HOST.cpu.info 2>&1
   rc=$?
   echo "######>cat /proc/cpuinfo" >> HARDINFO/$HOST.cpu.info 2>&1
   cat /proc/cpuinfo >> HARDINFO/$HOST.cpu.info 2>&1
   rc1=$?
   if [ $rc -ne 0 ] && [ $rc1 -ne 0 ] ; then
      echo "Failed to collec cpu information."
      sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "lscpu:Failed"
      rm HARDINFO/$HOST.cpu.info
   fi
}
##Hardware : collect memory information
function sdbMemory()
{
   echo "######>free -m" >> HARDINFO/$HOST.memory.info 2>&1
   free -m >> HARDINFO/$HOST.memory.info 2>&1
   rc=$?
   echo "######>cat /proc/meminfo" >> HARDINFO/$HOST.memory.info 2>&1
   cat /proc/meminfo >> HARDINFO/$HOST.memory.info 2>&1
   rc1=$?
   if [ $rc -ne 0 ] && [ $rc1 -ne 0 ] ; then
      echo "Failed to collect memory information."
      sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "free -m
      :Failed"
      rm HARDINFO/$HOST.memory.info
   fi
}
##Hardware : collect disk information
function sdbDisk()
{
   echo "######>lsblk" >> HARDINFO/$HOST.disk.info 2>&1
   lsblk >> HARDINFO/$HOST.disk.info 2>&1
   rc=$?
   echo "######>df -h" >> HARDINFO/$HOST.disk.info 2>&1
   df -h >> HARDINFO/$HOST.disk.info 2>&1
   rc1=$?
   if [ $rc -ne 0 ] && [ $rc1 -ne 0 ] ; then
      echo "Failed to collect disk information"
      sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "lsblk:Failed"
      rm HARDINFO/$HOST.disk.info
   fi
}
##Hardware : collect network card information
function sdbNetcard()
{
   echo "######>lspci|grep -i 'eth'" >> HARDINFO/$HOST.netcard.info 2>&1
   lspci|grep -i 'eth' >> HARDINFO/$HOST.netcard.info 2>&1
   rc=$?
   if [ $rc -ne 0 ] ; then
      echo "Failed to collect netcard information"	
      sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "lspci|grep -i
      'eth':Failed"
      rm HARDINFO/$HOST.netcard.info
   fi
}
##Hardware : collect mainboard information
function sdbMainboard()
{
   echo "######>lspci" >> HARDINFO/$HOST.mainboard.info 2>&1
   lspci >> HARDINFO/$HOST.mainboard.info 2>&1
   rc=$?
   echo "lspci -vv" >> HARDINFO/$HOST.mainboard.info 2>&1
   lspci -vv >> HARDINFO/$HOST.mainboard.info 2>&1
   rc1=$?
   if [ $rc -ne 0 ] && [ $rc1 -ne 0 ] ; then
      echo "Failed to collect mainboard information"
      sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "lspci:Failed"
      rm HARDINFO/$HOST.mainboard.info 
   fi

}
#**********************************************
#collect part of hardware information
#**********************************************
function sdbHardwareInfoPart()
{
   HOST=$1

   cpu=$2
   memory=$3
   disk=$4
   netcard=$5
   mainboard=$6

   if [ "$cpu" == "true" ] || [ "$memory" == "true" ] || [ "$disk" == "true" ] || [ "$netcard" == "true" ] || [ "$mainboard" == "true" ] ; then
      mkdir -p HARDINFO/
   else
      return 0
   fi

   if [ "$cpu" == "true" ] ; then
      sdbCpu
   fi

   if [ "$memory" == "true" ] ; then
      sdbMemory
   fi

   if [ "$disk" == "true" ] ; then
      sdbDisk
   fi

   if [ "$netcard" == "true" ] ; then
      sdbNetcard
   fi

   if [ "$mainboard" == "true" ] ; then
      sdbMainboard
   fi

#if folder don't have file ,then delete it
   lsfold=`ls HARDINFO/` >>/dev/null 2>&1
   if [ "$lsfold" == "" ] ; then
      rm -rf HARDINFO/
   else
      sdbEchoLog "EVENT" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Success to
         collect Hardware information"
   fi
}
#******************************************************
#collect operating system information all
#******************************************************
function sdbSystemInfoAll()
{
   HOST=$1

   mkdir -p OSINFO/

   sdbDiskManage
   sdbSystemOS
   sdbModules
   sdbEnvVar
   sdbNetworkInfo
   sdbProcess
   sdbLogin
   sdbLimit
   sdbVmstat

   #if folder don't have file ,then delete it
   lsfold=`ls OSINFO/` >>/dev/null 2>&1
   if [ "$lsfold" == "" ] ; then
      rm -rf OSINFO/
   else
      sdbEchoLog "EVENT" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Success to collect OS information"
   fi

}
##OSinfo : collect disk manage information
function sdbDiskManage()
{
   echo "######>df ./   " >> OSINFO/$HOST.diskmanage.sys
   df ./ >> OSINFO/$HOST.diskmanage.sys 2>&1
   rc=$?
   echo "######>mount   " >> OSINFO/$HOST.diskmanage.sys
   mount >> OSINFO/$HOST.diskmanage.sys 2>&1
   rc1=$?
   if [ $rc -ne 0 ] && [ $rc1 -ne 0 ] ; then 
      echo "Failed to collect disk manage information"
      sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "df ./:Failed"
      rm OSINFO/$HOST.diskmanage.sys
   fi
}
##OSinfo : collect system information 
function sdbSystemOS()
{
   echo "######>head -n 1 /etc/issue" >> OSINFO/$HOST.system.sys 2>&1
   head -n 1 /etc/issue >> OSINFO/$HOST.system.sys 2>&1
   rc=$?
   echo "######>cat /proc/version" >> OSINFO/$HOST.system.sys 2>&1
   cat /proc/version >> OSINFO/$HOST.system.sys 2>&1
   rc1=$?
   echo "######>hostname" >> OSINFO/$HOST.system.sys 2>&1
   hostname >> OSINFO/$HOST.system.sys 2>&1
   rc2=$?
   echo "######>getconf LONG_BIT" >> OSINFO/$HOST.system.sys 2>&1
   getconf LONG_BIT >> OSINFO/$HOST.system.sys 2>&1
   rc3=$?
   echo "######>ulimit -a" >> OSINFO/$HOST.system.sys 2>&1
   ulimit -a >> OSINFO/$HOST.system.sys 2>&1
   rc4=$?
   echo "######>lsb_release -a" >> OSINFO/$HOST.system.sys 2>&1
   lsb_release -a >> OSINFO/$HOST.system.sys 2>&1
   rc5=$?
   if [ $rc -ne 0 ] && [ $rc1 -ne 0 ] && [$rc2 -ne 0 ] && [ $rc3 -ne 0 ] && [ $rc4 -ne 0 ] && [ $rc5 -ne 0 ] ; then 
      echo "Failed to collect system information "
      sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "OSsys:Failed"
      rm OSINFO/$HOST.system.sys
   fi
}
##OSinfo : collect modules information
function sdbModules()
{
   echo "######>lsmod" >> OSINFO/$HOST.modules.sys 2>&1
   lsmod >> OSINFO/$HOST.modules.sys 2>&1
   rc=$?
   if [ $rc -ne 0 ] ; then
      echo "Failed to collect modules information"
      sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "lsmod:Failed"
      rm $HOST.modules.sys
   fi
}
##OSinfo : collect environment variable
function sdbEnvVar()
{
   echo "######>env" >> OSINFO/$HOST.environmentvar.sys 2>&1
   env >> OSINFO/$HOST.environmentvar.sys 2>&1
   rc=$?
   if [ $rc -ne 0 ] ; then
      echo "Failed to collect environment variable"
      sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "env:Failed"
      rm OSINFO/$HOST.environmentvar.sys
   fi
}
##OSinfo : collect network information
function sdbNetworkInfo()
{
   echo "######>netstat -s" >> OSINFO/$HOST.networkinfo.sys 2>&1
   netstat -s >> OSINFO/$HOST.networkinfo.sys 2>&1
   rc=$?
   echo "######>netstat" >> OSINFO/$HOST.networkinfo.sys 2>&1
   netstat >> OSINFO/$HOST.networkinfo.sys 2>&1
   rc1=$?
   if [ $rc -ne 0 ] && [ $rc1 -ne 0 ] ; then
      echo "Failed to collect network information"
      sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "netstat
      -s/netstat:Failed"
      rm OSINFO/$HOST.networkinfo.sys
   fi

}
##OSinfo : collet process information
function sdbProcess()
{
   echo "######>ps -elf|sort -rn" >> OSINFO/$HOST.process.sys 2>&1
   ps -elf|sort -rn >> OSINFO/$HOST.process.sys 2>&1
   rc=$?
   echo "######>ps aux" >> OSINFO/$HOST.process.sys 2>&1
   ps aux >> OSINFO/$HOST.process.sys 2>&1
   rc1=$?
   if [ $rc -ne 0 ] && [ $rc1 -ne 0 ] ; then
      echo "Failed to collect process information"
      sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "ps aux/ps
      -elf|sort -rn:Failed"
      rm OSINFO/$HOST.process.sys
   fi
}
##OSinfo : collect login information
function sdbLogin()
{
   echo "######>last" >> OSINFO/$HOST.logininfo.sys 2>&1
   last >> OSINFO/$HOST.logininfo.sys 2>&1
   rc=$? 
   echo "######>history" >> OSINFO/$HOST.logininfo.sys 2>&1
   history >> OSINFO/$HOST.logininfo.sys 2>&1
   rc1=$?
   if [ $rc -ne 0 ] && [ $rc1 -ne 0 ] ; then
      echo "Failed to collect login information"
      sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "last/history:Failed"
      rm OSINFO/$HOST.logininfo.sys
   fi
}
##OSinfo : collect limit information
function sdbLimit()
{
   echo "######>ulimit -a" >> OSINFO/$HOST.ulimit.sys 2>&1
   ulimit -a >> OSINFO/$HOST.ulimit.sys 2>&1
   rc=$?
   if [ $rc -ne 0 ] ; then
      echo "Failed to collect system limit information"
      sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "ulimit -a:Failed"
      rm OSINFO/$HOST.ulimit.sys
   fi
}
##OSinfo : collect vmstat information
function sdbVmstat()
{
   echo "######>vmstat" >> OSINFO/$HOST.vmstat.sys 2>&1
   vmstat >> OSINFO/$HOST.vmstat.sys 2>&1
   rc=$?
   if [ $rc -ne 0 ] ; then
      echo "Failed to collect vmstat information"
      sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "vmstat:Failed"
      rm OSINFO/$HOST.vmstat.sys
   fi

}
#*************************************************************
#collect part of operating system information ,front half
#*************************************************************
function sdbSystemInfoPartFore()
{
   HOST=$1

   diskmanage=$2
   osystem=$3
   kermode=$4
   env=$5
   network=$6

   if [ "$diskmanage" == "true" ] || [ "$osystem" == "true" ] || [ "$kermode" == "true" ] || [ "$env" == "true" ] || [ "$network" == "true" ] ; then
      mkdir -p OSINFO/
   else
      return 0
   fi

   if [ "$diskmanage" == "true" ] ; then
      sdbDiskManage
   fi

   if [ "$osystem" == "true" ] ; then
      sdbSystemOS
   fi

   if [ "$kermode" == "true" ] ; then
      sdbModules
   fi

   if [ "$env" == "true" ] ; then
      sdbEnvVar
   fi

   if [ "$network" == "true" ] ; then
      sdbNetworkInfo
   fi

   #if folder don't have file ,then delete it
   lsfold=`ls OSINFO/` >>/dev/null 2>&1
   if [ "$lsfold" == "" ] ; then
      rm -rf OSINFO/
   else
      sdbEchoLog "EVENT" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Sucess to collect OS information"
   fi

}
#*************************************************************
#collect end half part of Operating System information 
#*************************************************************
function sdbSystemInfoPartEnd()
{
   HOST=$1

   process=$2
   login=$3
   limit=$4
   vmstat=$5

   if [ "$process" == "true" ] || [ "$login" == "true" ] || [ "$limit" == "true" ] || [ "$vmstat" == "true" ] ; then
      mkdir -p OSINFO/
   else
      return 0
   fi

   if [ "$process" == "true" ] ; then
      sdbProcess
   fi

   if [ "$login" == "true" ] ; then
      sdbLogin
   fi

   if [ "$limit" == "true" ] ; then
      sdbLimit
   fi

   if [ "$vmstat" == "true" ] ; then
      sdbVmstat
   fi

   #if folder don't have file ,then delete it
   lsfold=`ls OSINFO/` >>/dev/null 2>&1
   if [ "$lsfold" == "" ] ; then
      rm -rf OSINFO/
   else
      sdbEchoLog "EVENT" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Sucess to
         collect OS information"
   fi
}


