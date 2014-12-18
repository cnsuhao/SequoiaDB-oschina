#!/bin/bash

#*******************************************************
#@function:install expect tool
#@time:2014/2/25
#	HXJ 
#*******************************************************
echo "*************************************************" 
echo "Please make sure that you have root permissions!
echo "Begin to install expect ...."
echo "*************************************************" 
chmod a+x expect
if [ $? -ne 0 ] ; then 
	echo "ERROR: Failed to chmod for expect " 
	exit 1 
fi 
cp expect /usr/local/bin/
if [ $? -ne 0 ] ; then 
	echo "ERROR: Failed to copy expect "
	exit 1 
fi 
cp libtcl8.4.so /usr/local/lib/
if [ $? -ne 0 ] ; then 
	echo "ERROR: Failed to copy libtcl8.4.so "
	exit 1 
fi 
mkdir -p /usr/local/lib/tcl8.4
if [ $? -ne 0 ] ; then 
	echo "ERROR: Failed to mkdir folder tcl8.4" 
	exit 1 
fi 
cp -R tcl/* /usr/local/lib/tcl8.4/
if [ $? -ne 0 ] ; then 
	echo "ERROR: Failed to copy tcl/* to folder tcl8.4"
	exit 1  
fi 
echo "**************************************************"
echo "Sucess to install expect !"
echo "**************************************************" 
