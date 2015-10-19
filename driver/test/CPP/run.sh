#!/bin/sh

SCRIPT=$(readlink -f "$0")
INSTALL_DIR=`dirname "$SCRIPT"`
#echo $INSTALL_DIR

$INSTALL_DIR/build_test/collection
$INSTALL_DIR/build_test/collectionspace
$INSTALL_DIR/build_test/cursor
$INSTALL_DIR/build_test/sdb
$INSTALL_DIR/build_test/cppbson
$INSTALL_DIR/build_test/debug
$INSTALL_DIR/build_test/domain
$INSTALL_DIR/build_test/lob
$INSTALL_DIR/build_test/procedure

$INSTALL_DIR/build_test/split
$INSTALL_DIR/build_test/rg
