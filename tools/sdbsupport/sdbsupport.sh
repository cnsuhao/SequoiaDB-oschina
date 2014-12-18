#!/bin/bash
BashPath=$(dirname $(readlink -f $0))
. $BashPath/sdbsupportfunc1.sh
. $BashPath/sdbsupportfunc2.sh
. /etc/default/sequoiadb
#variable in bash shell
#declare -a PORT
#declare -a DBPATH
#declare -a ROLE
#declare -a HOST
confpath=""
pHostNum=""

#user permission
USER=`whoami`
if [ "" == $USER ] ; then
   USER="sdbadmin"
fi

#gloable variable
hostName=""
svcPort=""
sysInfo="false"
snapShot="false"
hardInfo="false"

#sdblog and conf
sdbconf="false"
sdblog="false"
sdbcm="false"

#hardware information variable
cpu="false"
memory="false"
disk="false"
netcard="false"
mainboard="false"

#snapshot variable
rcPort="false"
group="false"
context="false"
session="false"
collection="false"
collectionspace="false"
database="false"
system="false"
catalog="false"

#operation system variable
diskmanage="false"
osystem="false"
module="false"
env="false"
IDE="false"
network="false"
progress="false"
login="false"
limit="false"
vmstat="false"
all="false"

#the parameter get where location
firstLoc=""
firstLoc=$1
thirdLoc=""
thirdLoc=$3

#the number of concurrent threads
thread=10
timeout=60

#get the number of parameter and what parameters is 
ParaNum=$#
ParaPass=$@

#check is local host run
Local=""

function Usage()
{
   echo "Command Options:" ;
   echo "    --help                 help information" ;
   echo "    -N [--hostname] arg    database host name [eg:-N hostname1:hostname2:.....]" ;
   echo "    -p [--svcport] arg     database sevice port [eg:-p 50000:30000:......]" ;
#   echo "    -t [--thread] arg      number of concurrent threads,default:10" ;
   echo "    -s [--snapshot]        snapshot of database database" ;
   echo "    -o [--osinfo]          operating system information" ;
   echo "    -h [--hardware]        hardware information" ;
   echo "    --all                  copy the all information of database" ;
   echo "    --conf                 collect config file of service port" ;
   echo "    --log                  collect sdb log file of service port" ;
   echo "    --cm                   collect sdbcm config and log file" ;
   echo "    --cpu                  host cpu information" ;
   echo "    --memory               host memory information" ;
   echo "    --disk                 host disk information" ;
   echo "    --netcard              host netcard information" ;
   echo "    --mainboard            host mainboard information" ;
   echo "    --catalog              catalog snapshot for database" ;
   echo "    --group                group of dababase information" ;
   echo "    --context              context snapshot" ;
   echo "    --session              session snapshot" ;
   echo "    --collection           collection snapshot" ;
   echo "    --collectionspace      collectionspace snapshot" ;
   echo "    --database             database snapshot" ;
   echo "    --system               system snapshot" ;
   echo "    --diskmanage           operating system disk management information" ;
   echo "    --basicsys             operating system basic information" ;
   echo "    --module               loadable kernel modules" ;
   echo "    --env                  operating system environment variable" ;
   echo "    --network              network information" ;
   echo "    --process              operating system process" ;
   echo "    --login                operating system users and history" ;
   echo "    --limit                ulimit used to limit the resources occupied shell startup process" ;
   echo "    --vmstat               Show the server status value of a given time interval" ;
#   echo "    --timeout              Set too much time to collect,default:50"

}

#the parameters can use  
optArg=`getopt -a -o N:p:t:sohH -l hostname,svcport,thread,snapshot,osinfo,hardware,help,conf,log,cm,cpu,memory,disk,netcard,mainboard,group,context,session,collection,collectionspace,database,system,diskmanage,basicsys,module,env,network,process,login,limit,vmstat,catalog,all,timeout: -- "$@"`

#check over the option of sdbsupport
rc=$?
if [ "$rc" == "1" ] ; then
   echo "The option don't have,please check by use '--help'!"
   exit 1
fi


eval set -- "$optArg"

while true
do
#eval set -- "$optArg"
   case $1 in
   -N|--hostname)
      hostName=$2
      shift
      ;;
   -p|--svcport)
      svcPort=$2
      shift
      ;;
   -t|--thread)
      thread=$2
      shift
      ;;
   --timeout)
      timeout=$2
      shift
      ;;
   -s|--snapshot)
      snapShot="true"
      ;;
   -o|--osinfo)
      sysInfo="true"
      ;;
   -h|--hardware)
      hardInfo="true"
      ;;
   --conf)
      sdbconf="true"
      ;;
   --log)
      sdblog="true"
      ;;
   --cm)
      sdbcm="true"
      ;;
   --cpu)
      cpu="true"
      ;;
   --memory)
      memory="true"
      ;;
   --disk)
      disk="true"
      ;;
   --netcard)
      netcard="true"
      ;; 
   --mainboard)
      mainboard="true"
      ;;
   --group)
      group="true"
      rcPort="true"
      ;;
   --context)
      context="true"
      rcPort="true"
      ;;
   --session)
      session="true"
      rcPort="true"
      ;;
   --collection)
      collection="true"
      rcPort="true"
      ;;
   --collectionspace)
      collectionspace="true"
      rcPort="true"
      ;;
   --database)
      database="true"
      rcPort="true"
      ;;
   --system)
      system="true"
      rcPort="true"
      ;;
   --diskmanage)
      diskmanage="true"
      ;;
   --basicsys)
      osystem="true"
      ;;
   --module )
      module="true"
      ;;
   --env)
      env="true"
      ;;
   --network)
      network="true"
      ;;
   --process)
      progress="true"
      ;;
   --login)
      login="true"
      ;;
   --limit)
      limit="true"
      ;;
   --vmstat)
      vmstat="true"
      ;;
   --catalog)
      catalog="true"
      ;;
   --all)
      all="true"
      ;;
   -H|--help)
      Usage
      exit 1
      ;;
   --)
      shift
      break
      ;;
   esac
shift
done

echo ""
echo "**********************Sdbsupport***************************"
echo "* This program run mode will collect all configuration and "
echo "* system environment information.Please make sure whether"
echo "* you need !"
echo "* Begin ....."
echo "***********************************************************"
echo ""


#******************************************************************************
#@Function : Check over environment
#******************************************************************************
#mv sdbsupport.log sdbsupport.log.1 >>sdbsupport.log 2>&1
rm -rf sdbsupport.log
rc=$?
if [ $rc -ne 0 ] ; then
   echo "Failed to remove sdbsupport.log file."
fi
#make sure the local path
dirpath=`pwd`
if [ "$dirpath" == "" ] ; then
   echo "Failed to get local directory."
   exit 1
fi
#inspect the environment of sequiaDB,localPath=where the sdbsuppor.sh run
localhost=`hostname`
localPath=$(dirname $(readlink -f $0))
#echo $localPath
if [ "$localhost" == "" ] || [ "$localPath" == "" ] ; then
   echo "Failed to get local host and local path."
   sdbEchoLog "ERROR" "$localhost/$0/${FUNCNAME}" "${LINENO}" "Failed to get local host:$localhost and local path:$localPath."
   exit 1
fi
#make a directory to story .tar.gz and .log.host file
mkdir -p $localPath/log
if [ $? -ne 0 ] ;then
   echo "Failed to create directory for sdbsupport.sh"
   sdbEchoLog "ERROR" "$localhost/$0/${FUNCNAME}" "${LINENO}" "Failed to create folder log in local path"
   exit 1
fi
#*************************************
# Give expect tool run permission
#*************************************
ls $localPath/expect/expect
if [ $? -ne 0 ] ; then
   echo "Don't have expect tools in path : $localPath/expect/expect"
   exit -1
fi
chmod 755 $localPath/expect/expect
if [ $? -ne 0 ] ; then
   echo "Failed to give expect tools run permission"
   exit -1
fi

#**********************************************************
#  export  export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$localPath/expect
#**********************************************************
libPath=`env | grep LD_LIBRARY_PATH | cut -d "=" -f 2`
libNum=`awk 'BEGIN{print split('"\"$libPath\""',libArr,":")}'`
for i in $(seq 1 $libNum)
do
   libpath[$i]=`awk 'BEGIN{split('"\"$libPath\""',libArr,":");print libArr['$i']}'`
   # awk 'BEGIN{split('"\"$cataddr\""',cateArr,",");print cateArr['$i']'}
   if [ "$localPath/expect/" != ${libpath[$i]} ] ; then
      export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$localPath/expect/
      if [ $? -ne 0 ] ; then
         echo "Failed to config system environment variable"
         exit -1
      fi
      echo "Success to export System environment variable : $localPath/expect/"
   else
      echo "System have environment variable : $localPath/expect/"
   fi
done

#***********************************************************
#get install path from /etc/default/sequoiadb file
#   NAME=sdbcm
#   SDBADMIN_USER=sdbadmin
#   INSTALL_DIR=/opt/sequoiadb
#***********************************************************
installpath=$INSTALL_DIR
if [ "" == $installpath ] ; then
   echo "Don't have file /etc/default/sequoiadb, inspect your "
   exit -1
fi

ls $installpath >/dev/null 2>&1
if [ $? -ne 0 ] ; then
   echo "Wrong install path ,Please check over the sdbsupport path and installpath!"
   sdbEchoLog "ERROR" "$localhost/$0/${FUNCNAME}" "${LINENO}" "Wrong install path:$installpath"
   exit 1
else
   sdbEchoLog "EVENT" "$localhost/$0/${FUNCNAME}" "${LINENO}" "Success to get install path:$installpath"
fi


cd $dirpath
if [ $? -ne 0 ] ; then
   echo "Failed to come back to sdbsupport directory"
   sdbEchoLog "ERROR" "$localhost/$0/${FUNCNAME}" "${LINENO}" "Failed to come back to sdbsupport directory:$localPath"
   exit 1
fi

#config file path
confpath=$installpath/conf/local
if ls $confpath >>/dev/null 2>&1
then
   retval=`ls $confpath`
   if [ "$retval" == "" ] ; then
      echo "Database don't have sdb nodes!"
      sdbEchoLog "ERROR" "$localhost/$0/${FUNCNAME}" "${LINENO}" "Database don't have sdb nodes. config path:$confpath"
      exit 1
   else
      echo "Check over Environment!"
      sdbEchoLog "EVENT" "$localhost/$0/${FUNCNAME}" "${LINENO}" "Database have nodes Correct config path:$confpath"
   fi
else
   echo "Wrong Config path:$confpath"
   sdbEchoLog "ERROR" "$localhost/$0/${FUNCNAME}" "${LINENO}" "Wrong Config path:$confpath"
   exit 1
fi

#************************************************************************
#@Function: create Number of concurrent threads
#@
#************************************************************************
fifo="/tmp/$$.fiofo"
mkfifo $fifo
if [ $? -ne 0 ] ; then
   echo "Failed to create FIFO,No parallel"
   sdbEchoLog "ERROR" "$localhost/$0/${FUNCNAME}" "${LINENO}" "Failed to create FIFO,No parallel"
else
   exec 6<>$fifo
   rm -rf $fifo
   for ((i=0;i<$thread;i++))
   do
      echo ""
   done >&6
   sdbEchoLog "EVENT" "$localhost/$0/${FUNCNAME}" "${LINENO}" "Success to create FIFO,parallel"
fi

#************************************************************************
#@Function : get quantity of all hosts and local sevic port
#@Var : HostNum   Exp : the number of hosts in the database
#@Var : PortNum   Exp : the number of local host's sevice port
#@Note : Array begin 1 count ,but such as file row begin 1, so use 1 begin 
#************************************************************************
cd $confpath
aloneRole=`find -name "*.conf"|xargs grep "\brole.*=.*standalone\b"|cut -d "/" -f 2`
coordRole=`find -name "*.conf"|xargs grep "\brole.*=.*coord\b"|cut -d "/" -f 2`
cataRole=`find -name "*.conf"|xargs grep "\brole.*=.*cata\b"|cut -d "/" -f 2`
dataRole=`find -name "*.conf"|xargs grep  "\brole.*=.*data\b"|cut -d "/" -f 2`
#echo "dataRole:$dataRole : aloneRole:$aloneRole"
cd $localPath
#*************************************************************************
#MODE:No Sdb  //Don't create database database,whether standalone and
#               group.Cannot collect ,exit shell!
#*************************************************************************
if [ "$aloneRole" == "" ] && [ "$coordRole" == "" ] && [ "$cataRole" == "" ] && [ "$dataRole"=="" ] ; then
   echo "Local host don't create database database"
   sdbEchoLog "ERROR" "$localhost/$0/${FUNCNAME}" "${LINENO}" "Local host don't create database database"
   exit 1
fi
#***************************************************************************
#MODE:standalone           //database have standalone database collect
#***************************************************************************
#if [ "$aloneRole" != "" ] ; then
#   echo "Node $aloneRole is standalone node"
#   sdbEchoLog "EVENT" "$localhost/$0/${FUNCNAME}" "${LINENO}" "Node $aloneRole is standalone node"
#   alonehost="standalone.$localhost"
#   dbpath=`grep -E "dbpath" $confpath/$aloneRole/sdb.conf|cut -d '=' -f 2`
#   sdbPortGather "$localhost" "$dbpath" "$aloneRole" "$installpath"
#   sdbSnapShotCataLog "$localhost" "$aloneRole" "$installpath"
#   sdbSnapShot "$localhost" "$aloneRole" "$installpath"
#   sdbHardwareInfoAll "$localhost" "$installpath"
#   sdbSystemInfoAll "$localhost" "$installpath"
#   #tar
#   sdbTarGzPack $alonehost
#fi

#***************************************************************************
#MODE:Group           //database database cluster/[group] only have coord
#***************************************************************************
if [ "$coordRole" != "" ] && [ "$cataRole" == "" ] && [ "$dataRole" == "" ] ;
then
   echo "database database cluster only have coord"
   sdbEchoLog "EVENT" "$localhost/$0/${FUNCNAME}" "${LINENO}" "database database cluster only have coord"
   dbpath=`grep -E "dbpath" $confpath/$coordRole/sdb.conf|cut -d '=' -f 2`
   sdbPortGather "$localhost" "$dbpath" "$coordRole" "$installpath"
   sdbHardwareInfoAll "$localhost" "$installpath"
   sdbSystemInfoAll "$localhost" "$installpath"
fi
#****************************************************************************
#MODE:Group   //database database cluster/[group] only have coord and cata
#****************************************************************************
if [ "$coordRole" != "" ] && [ "$cataRole" != "" ] && [ "$dataRole" == "" ] ;
then
   echo "database database cluster only have coord and cata"
   sdbEchoLog "EVENT" "$localhost/$0/${FUNCNAME}" "${LINENO}" "database database cluster only have coord and cata"
   dataRole=$coordRole
fi
#***************************************************************************
#MODE:Group           //Complete database database cluster/[group]
#***************************************************************************
if [ "$dataRole" != "" ] ; then
   echo "Complete database database cluster"
   sdbEchoLog "EVENT" "$localhost/$0/${FUNCNAME}" "${LINENO}" "Complete database database cluster"
   dataRole=$dataRole
fi
#catadrr : get the cata address and catch hosts

data=`echo $dataRole | cut -d " " -f 1`
#echo "date=$data...confpath=$confpath"
#echo "grep -E "catalogaddr" $confpath/$data/sdb.conf|cut -d '=' -f 2"
cataddr=`grep -E "catalogaddr.*=" $confpath/$data/sdb.conf|cut -d '=' -f 2`
#echo "cataddr=$cataddr"
HostNum=`awk 'BEGIN{print split('"\"$cataddr\""',cateArr,",")}'`
#echo "HostNum:$HostNum"
PortNum=`ls -l $confpath|grep "^d"|wc -l`

if [ "$HostNum" != "0" ] && [ "$PortNum" != "0" ] ; then
   sdbEchoLog "EVENT" "$localhost/$0/${FUNCNAME}" "${LINENO}" "Success to get the number of host in group and port in localhost"
else
   echo "No host and port,Please check!"
   sdbEchoLog "ERROR" "$localhost/$0/${FUNCNAME}" "${LINENO}" "No host and port,Please check!"
   exit 1
fi
#*******************************************************************************
#@Function : get all hosts in database and local host's port/dbpath/role 
#@Var : HOST      Exp : Array variable used to store hosts in database
#@Var : PORT      Exp : Array variable store local host's sevice port
#@Var : DBPATH    Exp : Array variable store local host's dbpath
#@Var : ROLE      Exp : Array variable store local hsot's sevice port's role 
#*******************************************************************************
for i in $(seq 1 $HostNum)
do
   hostcata[$i]=`awk 'BEGIN{split('"\"$cataddr\""',cateArr,",");print cateArr['$i']'}`
   HOST[$i]=`echo ${hostcata[$i]}|cut -d ":" -f 1 `
   if [ "${HOST[$i]}" == "$localhost" ] ; then
      for j in $(seq 1 $PortNum)
      do
         PortArr=`ls $confpath`
         PORT[$j]=`echo $PortArr|cut -d " " -f $j`
         if [ "${PORT[$j]}" == "$aloneRole" ] ; then
            PORT[$j]=""
            continue
         fi
         DBPATH[$j]=`grep -E "dbpath" $confpath/${PORT[$j]}/sdb.conf|cut -d '=' -f 2`
         #delete the space in config file and put in tmpconf
         sed -i 's/\ //g' $confpath/${PORT[$j]}/sdb.conf 
            ROLE[$j]=`grep -E "role=" $confpath/${PORT[$j]}/sdb.conf|cut -d '=' -f 2`
      done
   fi
done
DbPath=`echo ${DBPATH[@]}`
AllHost=`echo ${HOST[@]}`
AllPort=`echo ${PORT[@]}`
sdbEchoLog "EVENT" "$localhost/$0/${FUNCNAME}" "${LINENO}" "allHost:[$AllHost] allPort:[$AllPort] allDbpath:[$DbPath]"
#echo "${HOST[@]} ${PORT[@]} ${DBPATH[@]}"
#*************************************************************************************************
#@Function : Get parameter passed in and check over them wether or not correct,if don't have this 
#            Host or Port ,will delete the wrong host and port 
#@Var : pHostNum  Exp : quantity of parameter hosts, such as :--hostname ubunt-dev1:ubunt-dev2:
#@Var : pPortNum  Exp : quantity of parameter sevice port, such as:--svcport 51111:61111
#@Var : HostPara  Exp : Array variable to store hosts parameter
#@Var : PortPara  Exp : Array variable to store local hosts' sevice port
#*************************************************************************************************
pHostNum=`awk 'BEGIN{print split('"\"$hostName\""',hostarr,":")}'`
pPortNum=`awk 'BEGIN{print split('"\"$svcPort\""',portarr,":")}'`
#when have parameter ,but not --all ,we must specify the hosts[--hostname]
if [ $pHostNum -eq 0 ] && [ "$all" == "false" ] && [ "$firstLoc" != "" ] ; then
   echo "Warning ! Please specify hosts!"
   exit 1
fi
#Check over Host
for i in $(seq 1 $pHostNum)
do
   HostPara[$i]=`awk 'BEGIN{split('"\"$hostName\""',hostarr,":");print hostarr['$i']}'`
   HostNumAdd=$(($HostNum+1))
   for j in $(seq 1 $HostNumAdd)
   do
      if [ "${HostPara[$i]}" == "${HOST[$j]}" ] ; then
         break
      fi
      #******************************************************************************
      #Note:check the argument of HOST,when the host para not equal the HOST that we
      #     get from database config file .We put null in the para localed in Array.
      #******************************************************************************
      if [ $j -gt $HostNum ] ; then
         echo "WARNIGN,database don't have host:${HostPara[$i]}"
         sdbEchoLog "WARNING" "$localhost/$0/${FUNCNAME}" "${LINENO}" "Don't have host:${HostPara[$i]}"
         HostPara[$i]=""
      fi
   done
done
#Check over Port
for i in $(seq 1 $pPortNum)
do
   PortPara[$i]=`awk 'BEGIN{split('"\"$svcPort\""',portarr,":");print portarr['$i']}'`
   PortNumAdd=$(($PortNum+1))
   for j in $(seq 1 $PortNumAdd)
   do
      if [ "${PortPara[$i]}" == "${PORT[$j]}" ] ; then
         DbPath[$i]=${DBPATH[$j]}
         Role[$i]=${ROLE[$j]}
         break
      fi
      if [ $j -gt $PortNum ] ; then
         echo "WARNIGN,database don't have port:${PortPara[$i]}"
         sdbEchoLog "WARNING" "$localhost/$0/${FUNCNAME}" "${LINENO}" "Don't have port:${PortPara[$i]}"
         PortPara[$i]=""
      fi
   done
done
paraHost=`echo ${HostPara[@]}`
paraPort=`echo ${PortPara[@]}`
paraDbpath=`echo ${DbPath[@]}`
sdbEchoLog "EVENT" "$localhost/$0/${FUNCNAME}" "${LINENO}" "paraHost:[$paraHost] paraPort:[$paraPort] paraDbpath:[$paraDbpath]"
#***********************************************************************************
#@Function: get password of host that you begin to collect information
#@
#***********************************************************************************
if [ "$all" == "true" ] ; then
   for i in $(seq 1 $HostNum)
   do
      if [ "${HOST[$i]}" != "$localhost" ] ; then
         echo "The host $USER@${HOST[$i]}'s password :"
         read -s PASSWD[$i]
         sdbCheckPassword "${HOST[$i]}" "${PASSWD[$i]}" >> sdbsupport.log 2>&1
         retVal=$?
         echo "inspect password, rc = $retVal"
         while [ "$retVal" == "5" ]
         do
            PASSWD[$i]=""
            echo "Wrong password of host $USER@${HOST[$i]}, please enter again :"
            read -s PASSWD[$i]
            sdbCheckPassword "${HOST[$i]}" "${PASSWD[$i]}" >> sdbsupport.log 2>&1
            retVal=$?
            echo "inspect password, rc = $retVal"
         done
         #echo "password:" "${PASSWD[$i]}"
      fi
   done
   sdbEchoLog "EVENT" "$localhost/$0/${FUNCNAME}" "${LINENO}" "Check over password of all host,Correct."
fi

if [ "$pHostNum" -gt 0 ] && [ "$all" == "false" ] ; then
   for i in $(seq 1 $pHostNum)
   do
      if [ "${HostPara[$i]}" != "" ] && [ "${HostPara[$i]}" != "$localhost" ] ; then
         echo "The host $USER@${HostPara[$i]}'s password :"
         read -s PASSWD[$i]
         sdbCheckPassword "${HostPara[$i]}" "${PASSWD[$i]}"
         retVal=$?
         echo "inspect password, rc = $retVal"
         while [ "$retVal" == "5" ]
         do
            PASSWD[$i]=""
            echo "Wrong password of host $USER@${HostPara[$i]}, please enter again :"
            read -s PASSWD[$i]
            sdbCheckPassword "${HostPara[$i]}" "${PASSWD[$i]}"
            retVal=$?
            echo "inspect password, rc = "$retVal
         done
         #echo "password:" "${PASSWD[$i]}"
      fi
   done
   sdbEchoLog "EVENT" "$localhost/$0/${FUNCNAME}" "${LINENO}" "Check over password of specify host,Correct."
fi

#******************************************************************************
#@Function : Create Folder OSINFO/SDBNODES/SDBSNAPS/HARDINFO in local path
#@Fold : OSINFO   Exp : directory for Operation System Information
#@Fold : SDBNODES Exp : directory for database all nodes ,such as coord,cata,data
#@Fold : SDBSNAPS Exp : directory for database snapshot
#@Fold : HARDINFO Exp : directory for hardware information
#******************************************************************************
   rm -rf HARDINFO/ OSINFO/ SDBNODES/ SDBSNAPS/
   if [ $? -ne 0 ] ; then
      echo "Failed to remove folder HARDINFO/ OSINFO/ SDBNODES/ SDBSNAPS/"
      sdbEchoLog "ERROR" "$localhost/$0/${FUNCNAME}" "${LINENO}" "Failed to remove folder"
   else
      sdbEchoLog "EVENT" "$localhost/$0/${FUNCNAME}" "${LINENO}" "Success to remove the folder four"
   fi
#*************************************************************************************************
#@Function : Collect local host information about database,such as Dialog,
#				 Conf,Group,Snapshot,Hardware and System information !
#@Var : pHostNum	Exp :	quantity of parameter hosts, such as :--hostname ubunt-dev1:ubunt-dev2: 
#@Var : pPortNum	Exp :	quantity of parameter sevice port, such as:--svcport 51111:61111 
#@Var : HostPara	Exp : Array variable to store hosts parameter
#@Var : PortPara	Exp : Array variable to store local hosts' sevice port
#*************************************************************************************************
#>1.no para here:./sdbsupport.sh      
for i in $(seq 1 $HostNum)
do
   if [ "$firstLoc" == "" ] && [ "$localhost" == "${HOST[$i]}" ] ; then
      for j in $(seq 1 $PortNum)
      do
            #echo "localhost:$localhost:${PORT[$j]}"
            sdbEchoLog "EVENT" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Start collect localhost only:[HostNum:$HostNum]-[PortNume:$PortNum]"
	    sdbPortGather "${HOST[$i]}" "${DBPATH[$j]}" "${PORT[$j]}" "$installpath"
            if [ "${ROLE[$i]}" == "coord" ] && [ "$catalog" == "true" ] ; then
               sdbSnapShotCataLog "${HOST[$i]}" "${PORT[$j]}" "$installpath"
            fi
            sdbSnapShot "${HOST[$i]}" "${PORT[$j]}" "$installpath"
      done
      sdbHardwareInfoAll "${HOST[$i]}" "$installpath"
      sdbSystemInfoAll "${HOST[$i]}" "$installpath"
   fi
done

#>2.Parameter : --all
if [ "$all" == "true" ] ; then
   for i in $(seq 1 $HostNum)
   do
      if [ "${HOST[$i]}" == "$localhost" ] ; then
         for j in $(seq 1 $PortNum)
         do
            sdbPortGather "${HOST[$i]}" "${DBPATH[$j]}" "${PORT[$j]}" "$installpath"
            if [ "${ROLE[$i]}" == "coord" ] && [ "$catalog" == "true" ] ; then
               sdbSnapShotCataLog "${HOST[$i]}" "${PORT[$j]}" "$installpath"
            fi
            sdbSnapShot "${HOST[$i]}" "${PORT[$j]}" "$installpath"
         done
         sdbHardwareInfoAll "${HOST[$i]}" "$installpath"
         sdbSystemInfoAll "${HOST[$i]}" "$installpath"
         Local=$localhost
      fi
      read -u 6
      if [ "${HOST[$i]}" != "" ] && [ "${HOST[$i]}" != "$localhost" ] ; then
      #if [ "${HOST[$i]}" != "" ] ; then
      {
         sdbsupport="./sdbsupport.sh -N ${HOST[$i]} >> /dev/null 2>&1"
         #ssh host and collect information
         sdbExpectSshHosts "${HOST[$i]}" "${PASSWD[$i]}" "$localPath" "$sdbsupport" "$timeout"
         sdbExpectScpHosts "${HOST[$i]}" "$localPath" "${PASSWD[$i]}"
         sdbSupportLog "${HOST[$i]}" "$localPath" "${PASSWD[$i]}"
         sdbSSHRemove "${HOST[$i]}" "${PASSWD[$i]}" "$localPath"
         #echo "concurent $i"
         echo "" >&6
      }&
      fi
   done
   wait
fi

#>3.have para but not all : ./sdbsupport.sh -h htest1:htes2 -p 11810:50000
for i in $(seq 1 $pHostNum)
do
   if [ "$all" == "false" ] ; then
      read -u 6
      #HostPara[$i]=`awk 'BEGIN{split("'$hostName'",hostarr,":");print hostarr['$i']}'` 
      if [ "${HostPara[$i]}" == "$localhost" ] ; then
         #only have localhost ./sdbsupport.sh -h htest1
         Local=$localhost
         if [ "$thirdLoc" == "" ] ; then
            for j in $(seq 1 $PortNum)
            do
               sdbPortGather "${HostPara[$i]}" "${DBPATH[$j]}" "${PORT[$j]}" "$installpath"
               sdbSnapShotCataLog "${HostPara[$i]}" "${PORT[$j]}" "$installpath"
               sdbSnapShot "${HostPara[$i]}" "${PORT[$j]}" "$installpath"
            done
            sdbHardwareInfoAll "${HostPara[$i]}"
            sdbSystemInfoAll "${HostPara[$i]}"
         else
            #Para : svcPort ->have this Port [./sdbsupport.sh -h htest1 -p 11810]
            if [ $pPortNum -ne 0 ] ; then
               for k in $(seq 1 $pPortNum)
               do
                  if [ "${PortPara[$k]}" != "" ] ; then
                     #collect the sdbcm.conf/sdbcmd.log/sdbcm.log/sdb.conf/sdbdiag.log file
                     if [ "$sdbconf" == "true" ] || [ "$sdblog" == "true" ] || [ "$sdbcm" == "true" ] ; then
                        sdbPortGatherPart "${HostPara[$i]}" "${DbPath[$k]}" "${PortPara[$k]}" "$installpath" "$sdbconf" "$sdblog" "$sdbcm"
                     else
                        sdbPortGather "${HostPara[$i]}" "${DbPath[$k]}" "${PortPara[$k]}" "$installpath"
                     fi

                     if [ "${Role[$k]}" == "coord" ] && [ "$catalog" == "true" ] ; then
                        sdbCoordSnapShot "${HostPara[$i]}" "${PortPara[$k]}" "$installpath" "$catalog" "$group"
                     fi
                     #snapShot
                     if [ "$snapShot" == "true" ] ; then
                        sdbSnapShot "${HostPara[$i]}" "${PortPara[$k]}" "$installpath"
                     fi
                     if [ "$sysInfo" == "false" ] && [ "$rcPort" == "true" ] ; then
                        sdbSnapShotExtract "${HostPara[$i]}" "${PortPara[$k]}" "$context" "$session" "$collection" "$collectionspace" "$database" "$system" "$installpath"
                     fi
                  fi
               done
            else
               #Just have snapshot argument ,no port argument
               for k in $(seq 1 $PortNum)
               do
                  if [ "$sdbconf" == "true" ] || [ "$sdblog" == "true" ] || [ "$sdbcm" == "true" ] ; then
                     sdbPortGatherPart "${HostPara[$i]}" "${DBPATH[$k]}" "${PORT[$k]}" "$installpath" "$sdbconf" "$sdblog" "$sdbcm"
#else
#                     sdbPortGather "${HostPara[$i]}" "${DBPATH[$k]}" "${PORT[$k]}" "$installpath"
                  fi

                  if [ "${Role[$k]}" == "coord" ] ; then
                     sdbCoordSnapShot "${HostPara[$i]}" "${PORT[$k]}" "$installpath" "$catalog" "$group"
                  fi
                  if [ "$snapShot" == "true" ] ; then
                     sdbSnapShot "${HostPara[$i]}" "${PORT[$k]}" "$installpath"
                  fi
                  if [ "$sysInfo" == "false" ] && [ "$rcPort" == "true" ] ; then
                     sdbSnapShotExtract "${HostPara[$i]}" "${PORT[$k]}" "$context" "$session" "$collection" "$collectionspace" "$database" "$system" "$installpath"
                  fi
               done
            fi
            #Parameter:--sysinfo ; Collect all system information or collect part of system information !
            if [ "$sysInfo" == "true" ] ; then
               sdbSystemInfoAll "${HostPara[$i]}"
            else
               sdbSystemInfoPartFore "${HostPara[$i]}" "$diskmanage" "$osystem" "$module" "$env" "$network"
               sdbSystemInfoPartEnd "${HostPara[$i]}" "$progress" "$login" "$limit" "$vmstat"
            fi

            #Parameter:--hardinfo ; Collect all hardware information or collect part of system information !
            if [ "$hardInfo" == "true" ] ; then
               sdbHardwareInfoAll "${HostPara[$i]}"
            else
               sdbHardwareInfoPart "${HostPara[$i]}" "$cpu" "$memory" "$disk" "$netcard" "$mainboard"
            fi
            #echo "" >&6
         fi
      else
         #echo "the host not equal localhost"
         for n in $(seq 1 $ParaNum)
         do
            Para[$n]=`echo $ParaPass|cut -d " " -f $n`
            if [ "${Para[$n]}" == "-N" ] || [ "${Para[$n]}" == "--hostname" ] ; then
               Para[$n]=""
            fi
            if [ "${Para[$n]}" == "$hostName" ] ; then
               Para[$n]=""
            fi
         done
         if [ "${HostPara[$i]}" != "" ] ; then
            {
            sdbsupport="./sdbsupport.sh -N ${HostPara[$i]} ${Para[@]} >>/dev/null 2>&1"
            echo "Start to collect information from ${HostPara[$i]}..."
            sdbExpectSshHosts "${HostPara[$i]}" "${PASSWD[$i]}" "$localPath" "$sdbsupport" "$timeout"
            sdbExpectScpHosts "${HostPara[$i]}" "$localPath" "${PASSWD[$i]}"
            sdbSupportLog "${HostPara[$i]}" "$localPath" "${PASSWD[$i]}"
            sdbSSHRemove "${HostPara[$i]}" "${PASSWD[$i]}" "$localPath"
            echo "Clean Over."
            echo "" >&6
            }&
         fi
      fi
   fi
done
wait

#tar the all collect information in a packet
if [ "$firstLoc" == "" ] || [ "$Local" == "$localhost" ]; then
   sdbTarGzPack $localhost
fi

#clean environment
exec 6>&-
sdbEchoLog "EVENT" "$localhost/$0/${FUNCNAME}" "${LINENO}" "Collect information Over"
cp sdbsupport.log ./log/sdbsupport.log.$localhost >> /dev/null 2>&1
rc=$?
if [ $rc -ne 0 ] ;then
	echo "Failed to copy local sdbsupport.log to log folder."
fi

#copy log folder in sdbsupport directory to local directory
if [ "$localPath" != "$dirpath" ]; then
   cp -r $localPath/log $dirpath
   rc=$?
   if [ $rc -ne 0 ] ; then
      echo "Failed to copy information to local directory."
   fi
   rm -rf $localPath/log
   rc=$?
   if [ $rc -ne 0 ] ;then
      echo "Failed to remove the log folder in directory:$localPath"
   fi
fi
