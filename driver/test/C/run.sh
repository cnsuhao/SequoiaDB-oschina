#!/bin/sh

SCRIPT=$(readlink -f "$0")
INSTALL_DIR=`dirname "$SCRIPT"`
#echo $INSTALL_DIR
$INSTALL_DIR/build_test/collection
$INSTALL_DIR/build_test/collectionspace
$INSTALL_DIR/build_test/cursor
$INSTALL_DIR/build_test/sdb
#$INSTALL_DIR/build_test/snapshot
$INSTALL_DIR/build_test/concurrent
$INSTALL_DIR/build_test/shard
$INSTALL_DIR/build_test/debug
$INSTALL_DIR/build_test/cbson
