#!/bin/bash

###########################################
# parameter description:
# $1: dest path(e.g: /home/sequoiadb/sequoiadb-1.8)
# $2: build type(debug or release)
###########################################

function copy_file()
{
   cp $1 $2
   if [ $? -ne 0 ]; then
      echo "ERROR: Failed to copy the file!"
      exit 1;
   fi
}

function copy_folder()
{
   rsync -vaq --exclude=".svn" $1 $2
   if [ $? -ne 0 ]; then
      echo "ERROR: Failed to copy the folder!"
      exit 1;
   fi
}

################################## begin #####################################
cur_path=""
dir_name=$(dirname $0)
if [[ ${dir_name:0:1} != "/" ]]; then
   cur_path=$(pwd)/$dir_name
else
   cur_path=$dir_name
fi
script_path="$cur_path"
build_type=$2
sdb_path="$script_path/.."
pkg_src_tmp=$1
mkdir -p $pkg_src_tmp
rm -rf $pkg_src_tmp/*
copy_file $sdb_path/script/sequoiadb $pkg_src_tmp

#########################################
# folder: bin
#########################################
echo "collect the files of \"bin\""
src_dir_bin="$sdb_path/bin"
dest_dir_bin="$pkg_src_tmp/bin"
mkdir -p $dest_dir_bin
copy_file $src_dir_bin/sdb $dest_dir_bin
copy_file $src_dir_bin/sdbbp $dest_dir_bin
copy_file $src_dir_bin/sdbcm $dest_dir_bin
copy_file $src_dir_bin/sdbcmart $dest_dir_bin
copy_file $src_dir_bin/sdbcmd $dest_dir_bin
copy_file $src_dir_bin/sdbcmtop $dest_dir_bin
copy_file $src_dir_bin/sdbdpsdump $dest_dir_bin
copy_file $src_dir_bin/sdbexprt $dest_dir_bin
copy_file $src_dir_bin/sdbfmp $dest_dir_bin
copy_file $src_dir_bin/sdbimprt $dest_dir_bin
copy_file $src_dir_bin/sdbinspect $dest_dir_bin
copy_file $src_dir_bin/sdblist $dest_dir_bin
copy_file $src_dir_bin/sdbrestore $dest_dir_bin
copy_file $src_dir_bin/sdbstart $dest_dir_bin
copy_file $src_dir_bin/sdbstop $dest_dir_bin
copy_file $src_dir_bin/sdbtop $dest_dir_bin
copy_file $src_dir_bin/sequoiadb $dest_dir_bin
copy_file $sdb_path/script/sdbwsart $dest_dir_bin
copy_file $sdb_path/script/sdbwstop $dest_dir_bin

#########################################
# folder: conf
#########################################
echo "collect the files of \"conf\""
mkdir -p $pkg_src_tmp/conf/local
mkdir -p $pkg_src_tmp/conf/log
src_dir_conf_smp="$sdb_path/conf/samples"
dest_dir_conf_smp="$pkg_src_tmp/conf/samples"
mkdir -p $dest_dir_conf_smp
copy_file $src_dir_conf_smp/sdb.cat $dest_dir_conf_smp/
copy_file $src_dir_conf_smp/sdb.conf.catalog $dest_dir_conf_smp/
copy_file $src_dir_conf_smp/sdb.conf.coord $dest_dir_conf_smp/
copy_file $src_dir_conf_smp/sdb.conf.data $dest_dir_conf_smp/
copy_file $src_dir_conf_smp/sdb.conf.standalone $dest_dir_conf_smp/
copy_file $src_dir_conf_smp/sdbtop.xml $dest_dir_conf_smp/
copy_file $sdb_path/conf/sdbcm.conf $pkg_src_tmp/conf/

#########################################
# folder: doc
#########################################
#TODO************************************************
#TODO**************adjust the folder*****************
#TODO************************************************
#echo "collect the files of \"doc\""
#mkdir -p $pkg_src_tmp/doc
#copy_folder "$sdb_path/doc/out/*" "$pkg_src_tmp/doc"
#copy_folder "$sdb_path/doc/manual" "$pkg_src_tmp/doc"

#########################################
# folder: include
#########################################
echo "collect the files of \"include\""
src_dir_inc="$sdb_path/client/include"
dest_dir_inc="$pkg_src_tmp/include"
mkdir -p $dest_dir_inc
copy_file $src_dir_inc/base64c.h $dest_dir_inc
copy_folder "$src_dir_inc/bson" "$dest_dir_inc"
copy_file $src_dir_inc/client.h $dest_dir_inc
copy_file $src_dir_inc/client.hpp $dest_dir_inc
copy_file $src_dir_inc/core.h $dest_dir_inc
copy_file $src_dir_inc/core.hpp $dest_dir_inc
copy_file $src_dir_inc/fromjson.hpp $dest_dir_inc
copy_file $src_dir_inc/json2rawbson.h $dest_dir_inc
copy_file $src_dir_inc/jstobs.h $dest_dir_inc
copy_file $src_dir_inc/ossErr.h $dest_dir_inc
copy_file $src_dir_inc/ossFeat.h $dest_dir_inc
copy_file $src_dir_inc/ossFeat.hpp $dest_dir_inc
copy_file $src_dir_inc/ossTypes.h $dest_dir_inc
copy_file $src_dir_inc/ossTypes.hpp $dest_dir_inc
copy_file $src_dir_inc/spd.h $dest_dir_inc

#########################################
# folder: java
#########################################
echo "collect the files of \"java\""
mkdir -p $pkg_src_tmp/java/jdk
jdk_name=""
arch_info=`arch`
if [ "os_$arch_info" == "os_x86_64" ];then
   jdk_name="jdk_linux64"
elif [ "os_$arch_info" == "os_x86_32" ];then
   jdk_name="jdk_linux32"
elif [ "os_$arch_info" == "os_ppc64" ];then
   jdk_name="jdk_ppclinux64"
else
   echo "The platform is not supported!"
   exit 1
fi
copy_folder "$sdb_path/java/$jdk_name/*" "$pkg_src_tmp/java/jdk"
copy_file $sdb_path/driver/java/sequoiadb.jar $pkg_src_tmp/java

#########################################
# folder: lib
#########################################
#**************************************
#TODO: save the php version to file: 5.3.3,5.3.8,5.3.10,5.3.15,5.4.6,5.5.0
#*****************************************
echo "collect the files of \"lib\""
src_dir_lib="$sdb_path/client/lib"
dest_dir_lib="$pkg_src_tmp/lib"
mkdir -p $dest_dir_lib
if [ "sc_$build_type" == "sc_debug" ];then
   build_dir="dd"
else
   build_dir="normal"
fi
copy_file $src_dir_lib/libsdbcpp.so $dest_dir_lib
copy_file $src_dir_lib/libsdbc.so $dest_dir_lib
copy_file $src_dir_lib/libstaticsdbc.a $dest_dir_lib
copy_file $src_dir_lib/libstaticsdbcpp.a $dest_dir_lib
mkdir -p $dest_dir_lib/phplib
while read LINE
do
   if [[ -n "$LINE" ]];then
      copy_file $sdb_path/driver/php5/build/$build_dir/libsdbphp-$LINE.so $dest_dir_lib/phplib
   fi
done < $sdb_path/driver/php5/php_ver_linux.list

#########################################
# folder: license
#########################################
#**********************************
#TODO: where is notice.txt from?
#**********************************
echo "collect the files of \"license\""
src_dir_lcns="$sdb_path/license"
dest_dir_lcns="$pkg_src_tmp/license"
mkdir -p $dest_dir_lcns
copy_file $src_dir_lcns/license_en.txt $dest_dir_lcns
copy_file $src_dir_lcns/license_zh.txt $dest_dir_lcns

#########################################
# folder: samples
#########################################
echo "collect the files of \"samples\""
src_dir_smp_c="$sdb_path/client/samples/C"
dest_dir_smp_c="$pkg_src_tmp/samples/C"
mkdir -p $dest_dir_smp_c
copy_file $src_dir_smp_c/buildApp.bat $dest_dir_smp_c
copy_file $src_dir_smp_c/buildApp.sh $dest_dir_smp_c
copy_file $src_dir_smp_c/build.bat $dest_dir_smp_c
copy_file $src_dir_smp_c/common.c $dest_dir_smp_c
copy_file $src_dir_smp_c/common.h $dest_dir_smp_c
copy_file $src_dir_smp_c/connect.c $dest_dir_smp_c
copy_file $src_dir_smp_c/index.c $dest_dir_smp_c
copy_file $src_dir_smp_c/insert.c $dest_dir_smp_c
copy_file $src_dir_smp_c/query.c $dest_dir_smp_c
copy_file $src_dir_smp_c/readme.txt $dest_dir_smp_c
copy_file $src_dir_smp_c/README_zh $dest_dir_smp_c
copy_file $src_dir_smp_c/run.bat $dest_dir_smp_c
copy_file $src_dir_smp_c/run.sh $dest_dir_smp_c
copy_file $src_dir_smp_c/sampledb.c $dest_dir_smp_c
copy_file $src_dir_smp_c/sampledb.dat $dest_dir_smp_c
copy_file $src_dir_smp_c/snap.c $dest_dir_smp_c
copy_file $src_dir_smp_c/sql.c $dest_dir_smp_c
copy_file $src_dir_smp_c/subArrayLen.c $dest_dir_smp_c
copy_file $src_dir_smp_c/update.c $dest_dir_smp_c
copy_file $src_dir_smp_c/update_use_id.c $dest_dir_smp_c
copy_file $src_dir_smp_c/upsert.c $dest_dir_smp_c
src_dir_smp_cs="$sdb_path/client/samples/C#"
dest_dir_smp_cs="$pkg_src_tmp/samples/C#"
mkdir -p $dest_dir_smp_cs
copy_file $src_dir_smp_cs/buildApp.bat $dest_dir_smp_cs
copy_file $src_dir_smp_cs/BulkInsert.cs $dest_dir_smp_cs
copy_file $src_dir_smp_cs/Common.cs $dest_dir_smp_cs
copy_file $src_dir_smp_cs/Find.cs $dest_dir_smp_cs
copy_file $src_dir_smp_cs/Insert.cs $dest_dir_smp_cs
copy_file $src_dir_smp_cs/sdbcs.dll $dest_dir_smp_cs
src_dir_smp_cpp="$sdb_path/client/samples/CPP"
dest_dir_smp_cpp="$pkg_src_tmp/samples/CPP"
mkdir -p $dest_dir_smp_cpp
copy_file $src_dir_smp_cpp/buildApp.bat $dest_dir_smp_cpp
copy_file $src_dir_smp_cpp/buildApp.sh $dest_dir_smp_cpp
copy_file $src_dir_smp_cpp/common.cpp $dest_dir_smp_cpp
copy_file $src_dir_smp_cpp/common.hpp $dest_dir_smp_cpp
copy_file $src_dir_smp_cpp/connect.cpp $dest_dir_smp_cpp
copy_file $src_dir_smp_cpp/index.cpp $dest_dir_smp_cpp
copy_file $src_dir_smp_cpp/insert.cpp $dest_dir_smp_cpp
copy_file $src_dir_smp_cpp/query.cpp $dest_dir_smp_cpp
copy_file $src_dir_smp_cpp/readme.txt $dest_dir_smp_cpp
copy_file $src_dir_smp_cpp/README_zh $dest_dir_smp_cpp
copy_file $src_dir_smp_cpp/replicaGroup.cpp $dest_dir_smp_cpp
copy_file $src_dir_smp_cpp/run.sh $dest_dir_smp_cpp
copy_file $src_dir_smp_cpp/sql.cpp $dest_dir_smp_cpp
copy_file $src_dir_smp_cpp/update.cpp $dest_dir_smp_cpp
dest_dir_smp_java_smp="$pkg_src_tmp/samples/Java/com/sequoiadb/samples"
mkdir -p $dest_dir_smp_java_smp
copy_file $sdb_path/client/samples/Java/Readme $pkg_src_tmp/samples/Java
src_dir_smp_java_smp="$sdb_path/client/samples/Java/com/sequoiadb/samples"
copy_file $src_dir_smp_java_smp/Constants.java $dest_dir_smp_java_smp
copy_file $src_dir_smp_java_smp/CsAndClOperation.java $dest_dir_smp_java_smp
copy_file $src_dir_smp_java_smp/Delete.java $dest_dir_smp_java_smp
copy_file $src_dir_smp_java_smp/Index.java $dest_dir_smp_java_smp
copy_file $src_dir_smp_java_smp/Insert.java $dest_dir_smp_java_smp
copy_file $src_dir_smp_java_smp/log4j.properties $dest_dir_smp_java_smp
copy_file $src_dir_smp_java_smp/Query.java $dest_dir_smp_java_smp
copy_file $src_dir_smp_java_smp/Snapshot.java $dest_dir_smp_java_smp
copy_file $src_dir_smp_java_smp/Sql.java $dest_dir_smp_java_smp
copy_file $src_dir_smp_java_smp/Update.java $dest_dir_smp_java_smp
dest_dir_smp_js="$pkg_src_tmp/samples/JS"
mkdir -p $dest_dir_smp_js
copy_file $sdb_path/client/samples/JS/example.js $dest_dir_smp_js
src_dir_smp_php="$sdb_path/client/samples/PHP"
dest_dir_smp_php="$pkg_src_tmp/samples/PHP"
mkdir -p $dest_dir_smp_php
copy_file $src_dir_smp_php/server.php $dest_dir_smp_php
copy_file $src_dir_smp_php/show.htm $dest_dir_smp_php
copy_file $src_dir_smp_php/test_routine.php $dest_dir_smp_php
dest_dir_smp_php_css="$dest_dir_smp_php/css"
mkdir -p $dest_dir_smp_php_css
copy_file $src_dir_smp_php/css/style.css $dest_dir_smp_php_css
dest_dir_smp_php_js="$dest_dir_smp_php/js"
mkdir -p $dest_dir_smp_php_js
copy_file $src_dir_smp_php/js/common.js $dest_dir_smp_php_js
src_dir_pyt="$sdb_path/client/samples/Python"
dest_dir_pyt="$pkg_src_tmp/samples/Python"
mkdir -p $dest_dir_pyt
copy_file $src_dir_pyt/collectionspace.py $dest_dir_pyt
copy_file $src_dir_pyt/connect.py $dest_dir_pyt
copy_file $src_dir_pyt/index.py $dest_dir_pyt
copy_file $src_dir_pyt/insert.py $dest_dir_pyt
copy_file $src_dir_pyt/query.py $dest_dir_pyt
copy_file $src_dir_pyt/sql.py $dest_dir_pyt
copy_file $src_dir_pyt/update.py $dest_dir_pyt

#########################################
# folder: tools
#########################################
echo "collect the files of \"tools\""
src_dir_tls_sdbsppt="$sdb_path/tools/sdbsupport"
dest_dir_tls_sdbsppt="$pkg_src_tmp/tools/sdbsupport"
mkdir -p $dest_dir_tls_sdbsppt
copy_file $src_dir_tls_sdbsppt/readme.txt $dest_dir_tls_sdbsppt
copy_file $src_dir_tls_sdbsppt/sdbdpsdump $dest_dir_tls_sdbsppt
copy_file $src_dir_tls_sdbsppt/sdbsupportfunc1.sh $dest_dir_tls_sdbsppt
copy_file $src_dir_tls_sdbsppt/sdbsupportfunc2.sh $dest_dir_tls_sdbsppt
copy_file $src_dir_tls_sdbsppt/sdbsupport.sh $dest_dir_tls_sdbsppt
src_dir_tls_sdbsppt_expt="$src_dir_tls_sdbsppt/expect"
dest_dir_tls_sdbsppt_expt="$pkg_src_tmp/tools/sdbsupport/expect"
mkdir -p $dest_dir_tls_sdbsppt_expt
copy_file $src_dir_tls_sdbsppt_expt/expect $dest_dir_tls_sdbsppt_expt
copy_file $src_dir_tls_sdbsppt_expt/install.sh $dest_dir_tls_sdbsppt_expt
copy_file $src_dir_tls_sdbsppt_expt/libtcl8.4.so $dest_dir_tls_sdbsppt_expt
copy_file $src_dir_tls_sdbsppt_expt/readme.txt $dest_dir_tls_sdbsppt_expt
copy_folder "$src_dir_tls_sdbsppt_expt/tcl" "$dest_dir_tls_sdbsppt_expt"
dest_dir_tls_sdbsppt_lib="$pkg_src_tmp/tools/sdbsupport/lib"
mkdir -p $dest_dir_tls_sdbsppt_lib
copy_folder "$sdb_path/tools/sdbsupport/lib/tcl8.4" "$dest_dir_tls_sdbsppt_lib"
php_name=""
if [ "os_$arch_info" == "os_x86_64" ];then
   php_name="php_linux"
elif [ "os_$arch_info" == "os_x86_32" ];then
   php_name="php_linux"
elif [ "os_$arch_info" == "os_ppc64" ];then
   php_name="php_power"
else
   echo "The platform is not supported!"
   exit 1
fi
src_dir_tls_srv_php="$sdb_path/tools/server/$php_name"
dest_dir_tls_srv_php="$pkg_src_tmp/tools/server/php"
mkdir -p $dest_dir_tls_srv_php
copy_folder "$src_dir_tls_srv_php/*" "$dest_dir_tls_srv_php"

#########################################
# folder: www
#########################################
echo "collect the files of \"www\""
src_dir_www="$sdb_path/client/admin/admintpl"
dest_dir_www="$pkg_src_tmp/www"
mkdir -p $dest_dir_www
copy_file $src_dir_www/index.php $dest_dir_www
mkdir -p $pkg_src_tmp/www/cache
src_dir_www_css="$sdb_path/client/admin/admintpl/css"
dest_dir_www_css="$pkg_src_tmp/www/css"
mkdir -p $dest_dir_www_css
copy_file $src_dir_www_css/dtree.css $dest_dir_www_css
copy_file $src_dir_www_css/setup.css $dest_dir_www_css
copy_file $src_dir_www_css/style.css $dest_dir_www_css
src_dir_www_dist_css="$sdb_path/client/admin/admintpl/dist/css"
dest_dir_www_dist_css="$pkg_src_tmp/www/dist/css"
mkdir -p $dest_dir_www_dist_css
copy_file $src_dir_www_dist_css/bootstrap.css $dest_dir_www_dist_css
copy_file $src_dir_www_dist_css/bootstrap.min.css $dest_dir_www_dist_css
copy_file $src_dir_www_dist_css/bootstrap-theme.css $dest_dir_www_dist_css
copy_file $src_dir_www_dist_css/bootstrap-theme.min.css $dest_dir_www_dist_css
src_dir_www_dist_fonts="$sdb_path/client/admin/admintpl/dist/fonts"
dest_dir_www_dist_fonts="$pkg_src_tmp/www/dist/fonts"
mkdir -p $dest_dir_www_dist_fonts
copy_file $src_dir_www_dist_fonts/glyphicons-halflings-regular.eot $dest_dir_www_dist_fonts
copy_file $src_dir_www_dist_fonts/glyphicons-halflings-regular.svg $dest_dir_www_dist_fonts
copy_file $src_dir_www_dist_fonts/glyphicons-halflings-regular.ttf $dest_dir_www_dist_fonts
copy_file $src_dir_www_dist_fonts/glyphicons-halflings-regular.woff $dest_dir_www_dist_fonts
src_dir_www_dist_js="$sdb_path/client/admin/admintpl/dist/js"
dest_dir_www_dist_js="$pkg_src_tmp/www/dist/js"
mkdir -p $dest_dir_www_dist_js
copy_file $src_dir_www_dist_js/bootstrap.js $dest_dir_www_dist_js
copy_file $src_dir_www_dist_js/bootstrap.min.js $dest_dir_www_dist_js
src_dir_www_img="$sdb_path/client/admin/admintpl/images"
dest_dir_www_img="$pkg_src_tmp/www/images"
mkdir -p $dest_dir_www_img
copy_file $src_dir_www_img/addcatalog.png $dest_dir_www_img
copy_file $src_dir_www_img/addcl.png $dest_dir_www_img
copy_file $src_dir_www_img/addcs.png $dest_dir_www_img
copy_file $src_dir_www_img/adddata.png $dest_dir_www_img
copy_file $src_dir_www_img/addgroup.png $dest_dir_www_img
copy_file $src_dir_www_img/addindex.png $dest_dir_www_img
copy_file $src_dir_www_img/addnode.png $dest_dir_www_img
copy_file $src_dir_www_img/delcl.png $dest_dir_www_img
copy_file $src_dir_www_img/delcs.png $dest_dir_www_img
copy_file $src_dir_www_img/deldata.png $dest_dir_www_img
copy_file $src_dir_www_img/delete.png $dest_dir_www_img
copy_file $src_dir_www_img/delindex.png $dest_dir_www_img
copy_file $src_dir_www_img/export.png $dest_dir_www_img
copy_file $src_dir_www_img/finddata.png $dest_dir_www_img
copy_file $src_dir_www_img/groupplay.png $dest_dir_www_img
copy_file $src_dir_www_img/groupstop.png $dest_dir_www_img
copy_file $src_dir_www_img/link.png $dest_dir_www_img
copy_file $src_dir_www_img/loading.gif $dest_dir_www_img
copy_file $src_dir_www_img/logo.png $dest_dir_www_img
copy_file $src_dir_www_img/nodeplay.png $dest_dir_www_img
copy_file $src_dir_www_img/nodestop.png $dest_dir_www_img
copy_file $src_dir_www_img/refresh.png $dest_dir_www_img
copy_file $src_dir_www_img/splitcl.png $dest_dir_www_img
copy_file $src_dir_www_img/sql.png $dest_dir_www_img
copy_file $src_dir_www_img/status_1.png $dest_dir_www_img
copy_file $src_dir_www_img/status_2.png $dest_dir_www_img
copy_file $src_dir_www_img/status_3.png $dest_dir_www_img
copy_file $src_dir_www_img/Thumbs.db $dest_dir_www_img
copy_file $src_dir_www_img/tick.png $dest_dir_www_img
copy_file $src_dir_www_img/transformation.png $dest_dir_www_img
copy_file $src_dir_www_img/updatedata.png $dest_dir_www_img
src_dir_www_img_tree=$src_dir_www_img/tree
dest_dir_www_img_tree=$dest_dir_www_img/tree
mkdir -p $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/array.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/attribute.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/barchart.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/bool.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/cl.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/cs.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/database.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/data.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/empty.gif $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/file.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/groupdanger.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/grouperror.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/groupok.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/group.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/groupwarning.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/index.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/joinbottom.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/join.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/linechart.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/line.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/minusbottom.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/minus.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/nodeerror.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/nodeok.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/node.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/nolines_minus.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/nolines_plus.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/null.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/number.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/object.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/plusbottom.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/plus.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/string.png $dest_dir_www_img_tree
copy_file $src_dir_www_img_tree/Thumbs.db $dest_dir_www_img_tree
src_dir_www_js="$sdb_path/client/admin/admintpl/js"
dest_dir_www_js="$pkg_src_tmp/www/js"
mkdir -p $dest_dir_www_js
copy_file $src_dir_www_js/ajax.js $dest_dir_www_js
copy_file $src_dir_www_js/data.js $dest_dir_www_js
copy_file $src_dir_www_js/dtree.js $dest_dir_www_js
copy_file $src_dir_www_js/flotr2.js $dest_dir_www_js
copy_file $src_dir_www_js/flotr2.min.js $dest_dir_www_js
copy_file $src_dir_www_js/group.js $dest_dir_www_js
copy_file $src_dir_www_js/jquery.js $dest_dir_www_js
copy_file $src_dir_www_js/monitor.js $dest_dir_www_js
copy_file $src_dir_www_js/public.js $dest_dir_www_js
copy_file $src_dir_www_js/setup.js $dest_dir_www_js
copy_file $src_dir_www_js/tool.js $dest_dir_www_js
src_dir_www_php="$sdb_path/client/admin/admintpl/php"
dest_dir_www_php="$pkg_src_tmp/www/php"
mkdir -p $dest_dir_www_php
copy_file $src_dir_www_php/error_cn.php $dest_dir_www_php
copy_file $src_dir_www_php/error_en.php $dest_dir_www_php
copy_file $src_dir_www_php/html_conf.php $dest_dir_www_php
copy_file $src_dir_www_php/html_globalvar_data.php $dest_dir_www_php
copy_file $src_dir_www_php/html_globalvar_group.php $dest_dir_www_php
copy_file $src_dir_www_php/html-show.php $dest_dir_www_php
copy_file $src_dir_www_php/sdb-init.php $dest_dir_www_php
src_dir_www_php_ajax="$src_dir_www_php/ajax"
dest_dir_www_php_ajax="$dest_dir_www_php/ajax"
mkdir -p $dest_dir_www_php_ajax
copy_file $src_dir_www_php_ajax/a-connectlist_f.php $dest_dir_www_php_ajax
copy_file $src_dir_www_php_ajax/a-data_l.php $dest_dir_www_php_ajax
copy_file $src_dir_www_php_ajax/a-data_r.php $dest_dir_www_php_ajax
copy_file $src_dir_www_php_ajax/a-data_w.php $dest_dir_www_php_ajax
copy_file $src_dir_www_php_ajax/a-group_l.php $dest_dir_www_php_ajax
copy_file $src_dir_www_php_ajax/a-group_r.php $dest_dir_www_php_ajax
copy_file $src_dir_www_php_ajax/a-group_w.php $dest_dir_www_php_ajax
copy_file $src_dir_www_php_ajax/a-monitor_l.php $dest_dir_www_php_ajax
copy_file $src_dir_www_php_ajax/a-monitor_r.php $dest_dir_www_php_ajax
copy_file $src_dir_www_php_ajax/a-sql_r.php $dest_dir_www_php_ajax
src_dir_www_php_model="$src_dir_www_php/model"
dest_dir_www_php_model="$dest_dir_www_php/model"
mkdir -p $dest_dir_www_php_model
copy_file $src_dir_www_php_model/m-setup.php $dest_dir_www_php_model
src_dir_www_shell="$sdb_path/client/admin/admintpl/shell"
dest_dir_www_shell="$pkg_src_tmp/www/shell"
mkdir -p $dest_dir_www_shell
copy_file $src_dir_www_shell/phpexec.php $dest_dir_www_shell
copy_file $src_dir_www_shell/sequoiadbconfig.sh $dest_dir_www_shell
copy_file $src_dir_www_shell/sequoiadbdeploy.sh $dest_dir_www_shell
copy_file $src_dir_www_shell/sequoiadbfun1.sh $dest_dir_www_shell
copy_file $src_dir_www_shell/sequoiadbfun2.sh $dest_dir_www_shell
copy_file $src_dir_www_shell/sequoiadbphp.sh $dest_dir_www_shell
copy_file $src_dir_www_shell/sequoiadbsub.sh $dest_dir_www_shell
src_dir_www_smarty="$sdb_path/client/admin/admintpl/smarty"
dest_dir_www_smarty="$pkg_src_tmp/www/smarty"
mkdir -p $dest_dir_www_smarty
copy_file $src_dir_www_smarty/debug.tpl $dest_dir_www_smarty
copy_file $src_dir_www_smarty/SmartyBC.class.php $dest_dir_www_smarty
copy_file $src_dir_www_smarty/Smarty.class.php $dest_dir_www_smarty
src_dir_www_smarty_plugins="$src_dir_www_smarty/plugins"
dest_dir_www_smarty_plugins="$dest_dir_www_smarty/plugins"
mkdir -p $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/block.textformat.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/function.counter.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/function.cycle.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/function.fetch.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/function.html_checkboxes.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/function.html_image.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/function.html_options.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/function.html_radios.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/function.html_select_date.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/function.html_select_time.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/function.html_table.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/function.mailto.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/function.math.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifier.capitalize.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifiercompiler.cat.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifiercompiler.count_characters.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifiercompiler.count_paragraphs.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifiercompiler.count_sentences.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifiercompiler.count_words.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifiercompiler.default.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifiercompiler.escape.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifiercompiler.from_charset.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifiercompiler.indent.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifiercompiler.lower.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifiercompiler.noprint.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifiercompiler.string_format.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifiercompiler.strip.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifiercompiler.strip_tags.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifiercompiler.to_charset.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifiercompiler.unescape.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifiercompiler.upper.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifiercompiler.wordwrap.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifier.date_format.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifier.debug_print_var.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifier.escape.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifier.regex_replace.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifier.replace.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifier.spacify.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/modifier.truncate.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/outputfilter.trimwhitespace.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/shared.escape_special_chars.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/shared.literal_compiler_param.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/shared.make_timestamp.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/shared.mb_str_replace.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/shared.mb_unicode.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/shared.mb_wordwrap.php $dest_dir_www_smarty_plugins
copy_file $src_dir_www_smarty_plugins/variablefilter.htmlspecialchars.php $dest_dir_www_smarty_plugins
src_dir_www_smarty_sysplg="$src_dir_www_smarty/sysplugins"
dest_dir_www_smarty_sysplg="$dest_dir_www_smarty/sysplugins"
mkdir -p $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_cacheresource_custom.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_cacheresource_keyvaluestore.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_cacheresource.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_config_source.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_cacheresource_file.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_append.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_assign.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compilebase.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_block.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_break.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_call.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_capture.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_config_load.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_continue.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_debug.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_eval.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_extends.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_foreach.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_for.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_function.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_if.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_include.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_include_php.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_insert.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_ldelim.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_nocache.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_private_block_plugin.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_private_function_plugin.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_private_modifier.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_private_object_block_function.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_private_object_function.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_private_print_expression.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_private_registered_block.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_private_registered_function.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_private_special_variable.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_rdelim.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_section.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_setfilter.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_compile_while.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_config_file_compiler.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_configfilelexer.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_configfileparser.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_config.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_data.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_debug.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_filter_handler.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_function_call_handler.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_get_include_path.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_nocache_insert.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_parsetree.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_resource_eval.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_resource_extends.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_resource_file.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_resource_php.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_resource_registered.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_resource_stream.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_resource_string.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_smartytemplatecompiler.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_templatebase.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_templatecompilerbase.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_templatelexer.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_templateparser.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_template.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_utility.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_internal_write_file.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_resource_custom.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_resource.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_resource_recompiled.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_resource_uncompiled.php $dest_dir_www_smarty_sysplg
copy_file $src_dir_www_smarty_sysplg/smarty_security.php $dest_dir_www_smarty_sysplg
src_dir_www_tmplt="$sdb_path/client/admin/admintpl/templates"
dest_dir_www_tmplt="$pkg_src_tmp/www/templates"
mkdir -p $dest_dir_www_tmplt
copy_file $src_dir_www_tmplt/data.html $dest_dir_www_tmplt
copy_file $src_dir_www_tmplt/foot.html $dest_dir_www_tmplt
copy_file $src_dir_www_tmplt/group.html $dest_dir_www_tmplt
copy_file $src_dir_www_tmplt/header.html $dest_dir_www_tmplt
copy_file $src_dir_www_tmplt/index.html $dest_dir_www_tmplt
copy_file $src_dir_www_tmplt/monitor.html $dest_dir_www_tmplt
copy_file $src_dir_www_tmplt/setup.html $dest_dir_www_tmplt
copy_file $src_dir_www_tmplt/tool.html $dest_dir_www_tmplt
src_dir_www_tmplt_ajax="$src_dir_www_tmplt/ajax"
dest_dir_www_tmplt_ajax="$dest_dir_www_tmplt/ajax"
mkdir -p $dest_dir_www_tmplt_ajax
copy_file $src_dir_www_tmplt_ajax/a-connectlist_f.html $dest_dir_www_tmplt_ajax
copy_file $src_dir_www_tmplt_ajax/a-data_l.html $dest_dir_www_tmplt_ajax
copy_file $src_dir_www_tmplt_ajax/a-data_r.html $dest_dir_www_tmplt_ajax
copy_file $src_dir_www_tmplt_ajax/a-data_w.html $dest_dir_www_tmplt_ajax
copy_file $src_dir_www_tmplt_ajax/a-group_l.html $dest_dir_www_tmplt_ajax
copy_file $src_dir_www_tmplt_ajax/a-group_r.html $dest_dir_www_tmplt_ajax
copy_file $src_dir_www_tmplt_ajax/a-group_w.html $dest_dir_www_tmplt_ajax
copy_file $src_dir_www_tmplt_ajax/a-monitor_l.html $dest_dir_www_tmplt_ajax
copy_file $src_dir_www_tmplt_ajax/a-sql_r.html $dest_dir_www_tmplt_ajax
src_dir_www_tmplt_tool="$src_dir_www_tmplt/tool"
dest_dir_www_tmplt_tool="$dest_dir_www_tmplt/tool"
mkdir -p $dest_dir_www_tmplt_tool
copy_file $src_dir_www_tmplt_tool/tool-data.html $dest_dir_www_tmplt_tool
copy_file $src_dir_www_tmplt_tool/tool-group.html $dest_dir_www_tmplt_tool
copy_file $src_dir_www_tmplt_tool/tool-monitor.html $dest_dir_www_tmplt_tool
copy_file $src_dir_www_tmplt_tool/tool-setup.html $dest_dir_www_tmplt_tool
mkdir -p $pkg_src_tmp/www/templates_c
src_dir_www_user="$sdb_path/client/admin/admintpl/user"
dest_dir_www_user="$pkg_src_tmp/www/user"
mkdir -p $dest_dir_www_user
copy_file $src_dir_www_user/userlist.ult $dest_dir_www_user

