#!/bin/bash

#check over the password is right or wrong
function sdbCheckPassword()
{
   HOST=$1
   PASSWD=$2

   $localPath/expect/expect -c "
      set timeout 20 ;
      spawn ssh $USER@$HOST ;
      expect {
         \"*yes/no*\" ; {send \"yes\r\" ; exp_continue}
         \"assword\" ; {send \"$PASSWD\r\" ;exp_continue}
         \"*denied*\" ; { exit 5;exp_continue}
         \"*login*\" ; {send \"exit\r\" ;exp_continue}
         eof
         {
            send_user \"eof\n\" ;
         }
      }
                        " >>/dev/null 2>&1
}

#ssh host and run sdbsupport
function sdbExpectSshHosts()
{
   HOST=$1
   PASSWD=$2
   localPath=$3
   sdbsupport=$4
   timeout=$5
   #echo "timeout:$timeout"

   endflag="echo \"Too much time\""
   $localPath/expect/expect -c   "
      set timeout -1 ;
      spawn ssh $USER@$HOST ;
      expect {
         \"*yes/no*\";{send \"yes\n\";exp_continue}
         \"*assword\";{send \"$PASSWD\n\";exp_continue}
         \"*login*\";{send \"cd $localPath\r\n\";send \"chmod +x sdbsupport.sh\r\n\";send \"$sdbsupport\r\n\";send \"\r\n\";send \"mv sdbsupport.log sdbsupport.log.$HOST\r\n\";send \"\r\n\";send \"exit\r\n\";exp_continue}
         \"No such file\";{exit 2 ;}
         \"Permission denied\";{exit 13;}
      eof
         {
           send_user \"eof\n\";
         }
      }
                              " >>/dev/null 2>&1

   rc=$?
   echo return:$rc
   if [ $rc -eq 2 ] ; then
      echo "Host:$HOST don't have directory:$localPath or file:sdbsupport.sh"
      sdbEchoLog "ERROR" "$0/$HOST/${FUNCNAME}" "${LINENO}" "Host:$HOST don't have directory:$localPath or file:sdbsupport.sh"
      continue
  elif [ $rc -eq 13 ] ; then
      echo "run sdbsupport tool Permission denied,please check."
      sdbEchoLog "ERROR" "$0/$HOST/${FUNCNAME}" "${LINENO}" "run sdbsupport tool Permission denied,please check."
      continue
   else
      echo "Success to collect information from $HOST"
      sdbEchoLog "Event" "$0/$HOST/${FUNCNAME}" "${LINENO}" "Success to collect information from $HOST"
   fi
#  if [ "$rc" == "4" ] ; then
#     echo "Run time out,please take too much time in host : $HOST"
#     sdbEchoLog "ERROR" "$0/$HOST/${FUNCNAME}" "${LINENO}" "Run time out,please take too much time in host : $HOST"
#  else
#     echo "Success to run sdbsupport.sh in $HOST"
#      sdbEchoLog "ERROR" "$0/$HOST/${FUNCNAME}" "${LINENO}" "Success to run sdbsupport"
#  fi
}

function sdbTarGzPack()
{
   HOST=$1
   date=`date '+%y%m%d-%H%M%S'`
   hard="true"
   sdbnode="true"
   osinfo="true"
   sdbsnap="true"

   Folder="$HOST-$date"
#echo "Begin to packaging and compression"
   mkdir -p $Folder/
   if [ $? -ne 0 ] ; then
      echo "Failed to create foler !"
      sdbEchoLog "ERROR" "$0/$HOST/${FUNCNAME}" "${LINENO}" "Failed to create foler !"
      exit 1
   fi

   if ls HARDINFO/ >>/dev/null 2>&1
   then
      hard="false"
      mv HARDINFO/ ./$Folder/
   fi

   if ls SDBNODES/ >>/dev/null 2>&1
   then
      sdbnode="false"
      mv SDBNODES/ ./$Folder/
   fi

   if ls OSINFO/ >>/dev/null 2>&1
   then
      osinfo="false"
      mv OSINFO/ ./$Folder/
   fi

   if ls SDBSNAPS/ >>/dev/null 2>&1
   then
      sdbsnap="false"
      mv SDBSNAPS/ ./$Folder/
   fi

   if [ "$hard" == "true" ] && [ "$sdbnode" == "true" ] && [ "$osinfo" == "true" ] && [ "$sdbsnap" == "true" ] ; then
      echo "Error,Failed to collect $HOST information "
      sdbEchoLog "ERROR" "$0/$HOST/${FUNCNAME}" "${LINENO}" "Error,Failed to collect $HOST information "
      rm -rf ./$Folder/
      exit 1
   fi

   if [ $? -ne 0 ] ; then
      echo "Failed to move the collected information to folder"
      sdbEchoLog "ERROR" "$0/$HOST/${FUNCNAME}" "${LINENO}" "Error,Failed to move $HOST information to folder"
      exit 1
   fi

   tar -zcvf $Folder.tar.gz ./$Folder/ >>/dev/null 2>&1

   if [ $? -ne 0 ] ; then
      echo "Failed to packaging and compression"
      sdbEchoLog "ERROR" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Failed to packaging and compression "
      exit 1
   else
      echo "Complete to packaging and compression"
      sdbEchoLog "EVENT" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Success to Complete to packaging and compression"
   fi

   mv $Folder.tar.gz $localPath/log
   if [ $? -ne 0 ] ; then
      echo "Failed to move pack-info to log folder."
   fi
   rm -rf ./$Folder/
}

function sdbExpectScpHosts()
{
   HOST=$1
   localPath=$2
   PASSWD=$3

#scp -r root@$HOST:$localPath/$HOST.tar.gz ./

   $localPath/expect/expect -c"
      set timeout -1 ;
      spawn scp -r $USER@$HOST:$localPath/log/*$HOST*.tar.gz $localPath/log/ ;
      expect {
         \"*yes/no*\";{send \"yes\n\";exp_continue}
         \"*assword\";{send \"$PASSWD\n\";exp_continue}
         timeout ;{exit 4;}
         eof
         {
            send_user \"eof\n\";
         }
      }
                              " >>/dev/null 2>&1
      if [ "$rc" == "4" ] ; then
         echo "Failed to copy $HOST:$localPath/*$HOST*.tar.gz"
         sdbEchoLog "EVENT" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Failed to scp $USER@$HOST:$localPath/*$HOST*.tar.gz"
      else
         echo "Success to copy information from $HOST"
         sdbEchoLog "EVENT" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Success to copy information from $HOST"
      fi

}

function sdbSupportLog()
{
   HOST=$1
   localPath=$2
   PASSWD=$3

   $localPath/expect/expect -c"
         set timeout -1 ;
         spawn scp -r $USER@$HOST:$localPath/sdbsupport.log.$HOST $localPath/log/ ;
         expect {
            \"*yes/no*\";{send \"yes\n\";exp_continue}
            \"*assword\";{send \"$PASSWD\n\";exp_continue}
            timeout ;{exit 4;}
            eof
            {
               send_user \"eof\n\";
            }
         }
                           " >>/dev/null 2>&1
   if [ "$rc" == "4" ] ; then
      echo "Failed to copy $HOST:$localPath/sdbsupport.log"
      sdbEchoLog "EVENT" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Failed to scp $USER@$HOST:$localPath/sdbsupport.log"
   else
      echo "Success to copy sdbsupport.log"
      sdbEchoLog "EVENT" "$HOST/$0/${FUNCNAME}" "${LINENO}" "Success to copy sdbsupport.log"
   fi

}

function sdbSSHRemove()
{
   HOST=$1
   PASSWD=$2
   localPath=$3

   $localPath/expect/expect -c"
      set timeout -1 ;
      spawn ssh $USER@$HOST ;
      expect {
         \"*yes/no*\";{send \"yes\n\";exp_continue}
         \"*assword\";{send \"$PASSWD\n\";exp_continue}
         \"*login*\";{send \"cd $localPath\r\n\";send \"rm -rf log/ sdbsupport.log.$HOST\r\n\";send \"\r\";send \"exit\r\" ;}
         \"No such file\";{exit 2 ;}
         \"Permission denied\";{exit 13;}
         eof
         {
           send_user \"eof\n\";
         }
      }

                           " >>/dev/null 2>&1
   rc=$?
   echo return:$rc
   if [ $rc -eq 2 ] ; then
      echo "Host:$HOST don't have directory:$localPath or file:sdbsupport.sh"
      sdbEchoLog "ERROR" "$0/$HOST/${FUNCNAME}" "${LINENO}" "Host:$HOST don't have directory:$localPath or file:sdbsupport.sh"
      continue
   elif [ $rc -eq 13 ] ; then
      echo "clean ENV Permission denied,please check."
      sdbEchoLog "ERROR" "$0/$HOST/${FUNCNAME}" "${LINENO}" "Permission denied,please check."
      continue
   else
      echo "Success to clean $HOST's Environment."
      sdbEchoLog "Event" "$0/$HOST/${FUNCNAME}" "${LINENO}" "Success to clean $HOST's Environment."
   fi
}

function sdbEchoLog()
{
   Date=`date +%Y-%m-%d-%H:%M:%S.%N`

   echo "Level: $1"
   echo "Date: $Date"
   echo "File/Function: $2"
   echo "Line: $3"
   echo "Message: "
   echo "$4"
   echo ""
} >> sdbsupport.log
