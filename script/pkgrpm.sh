#!/bin/bash

###########################################
# script parameter description:
# $1: source files's path
###########################################

function err_exit()
{
   if [ $1 -ne 0 ]; then
      exit 1;
   fi
}

function get_macro_val_frm_line()
{
   if [[ "def$2" != def#define ]]; then
      return ;
   fi
   if [[ "def$1" != "def$3" ]]; then
      return ;
   fi
   echo "$4"
}

##################################
# function parameter description:
# $1: file name
# $2: macro name
##################################
function get_macro_val()
{
   val=$2
   while read LINE
   do
      rs=`get_macro_val_frm_line $val $LINE`
      if [[ -n "$rs" ]]; then
         val=$rs;
         break;
      fi
   done < $1
   if [[ "def$2" != "def$val" ]];then
      val=`get_macro_val $1 $val`
   fi
   echo "$val"
}

cur_path=""
dir_name=$(dirname $0)
if [[ ${dir_name:0:1} != "/" ]]; then
   cur_path=$(pwd)
fi
files_src_path=$1
script_path="$cur_path/$dir_name"
sdb_path="$script_path/.."
pkg_root_path="$sdb_path/package"
pkg_tmp_path=$pkg_root_path/tmp
pkg_output_path="$pkg_root_path/output"
pkg_conf_file=$pkg_root_path/conf/rpm/sequoiadb.spec
mkdir -p $pkg_tmp_path

ver_name="SDB_ENGINE_VERISON_CURRENT"
subver_name="SDB_ENGINE_SUBVERSION_CURRENT"
ver_info_file=$sdb_path/SequoiaDB/engine/include/ossVer.h
begin_ver=`get_macro_val $ver_info_file $ver_name`
sub_ver=`get_macro_val $ver_info_file $subver_name`
sdb_name="sequoiadb-$begin_ver.$sub_ver"

echo "package the source files"
pkg_src_path="$pkg_tmp_path/$sdb_name"
mkdir -p $pkg_src_path
cp -rf $files_src_path/* $pkg_src_path
err_exit $?
tar -czf $pkg_tmp_path/$sdb_name.tar.gz -C $pkg_tmp_path $sdb_name --remove-files
err_exit $?
echo "prepare the spec file"
mkdir -p $pkg_tmp_path/rpm/SPECS
cp -f $pkg_conf_file $pkg_tmp_path/rpm/SPECS
sed -i "s/SDB_ENGINE_VERISON_CURRENT/$begin_ver/g" $pkg_tmp_path/rpm/SPECS/sequoiadb.spec
sed -i "s/SDB_ENGINE_SUBVERSION_CURRENT/$sub_ver/g" $pkg_tmp_path/rpm/SPECS/sequoiadb.spec
echo "build the RPM package"
mkdir -p $pkg_tmp_path/rpm/SOURCES
rm -rf $pkg_tmp_path/rpm/SOURCES/*
mv $pkg_tmp_path/$sdb_name.tar.gz $pkg_tmp_path/rpm/SOURCES
rpmbuild --rmsource --define "_topdir $pkg_tmp_path/rpm" -bb $pkg_tmp_path/rpm/SPECS/sequoiadb.spec
if [[ $? -ne 0 ]]; then
   exit 1;
fi
mkdir -p $pkg_output_path
arch_info=`arch`
if [[ -z "$arch_info" ]];then
   echo "ERROR: failed to get arch info!"
   exit 1;
fi
rm -rf $pkg_output_path/RPMS
mv $pkg_tmp_path/rpm/RPMS $pkg_output_path
rm -rf $pkg_tmp_path/rpm
