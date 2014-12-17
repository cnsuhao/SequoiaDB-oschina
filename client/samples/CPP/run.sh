#!/bin/bash
echo "###############building file..."
./buildApp.sh connect
./buildApp.sh query
./buildApp.sh replicaGroup
./buildApp.sh index
./buildApp.sh sql
./buildApp.sh insert
./buildApp.sh update
./buildApp.sh lob

echo "###############running connect..."
./build/connect localhost 11810 "" ""
echo "###############running snap..."
./build/query localhost 11810 "" ""
#echo "###############running replicaGroup..."
#./build/replicaGroup localhost 11810 "" ""
echo "###############running index..."
./build/index localhost 11810 "" ""
echo "###############running sql..."
./build/sql localhost 11810 "" ""
echo "###############running insert..."
./build/insert localhost 11810 "" ""
echo "###############running update..."
./build/update localhost 11810 "" ""
echo "###############running lob..."
./build/lob localhost 11810 "" ""



echo "###############running connect.static..."
./build/connect.static localhost 11810 "" ""
echo "###############running snap.static..."
./build/query.static localhost 11810 "" ""
#echo "###############running replicaGroup.static..."
#./build/replicaGroup.static localhost 11810 "" ""
echo "###############running index.static..."
./build/index.static localhost 11810 "" ""
echo "###############running sql.static..."
./build/sql.static localhost 11810 "" ""
echo "###############running insert.static..."
./build/insert.static localhost 11810 "" ""
echo "###############running update.staic..."
./build/update.static localhost 11810 "" ""
echo "###############running lob.staic..."
./build/lob.static localhost 11810 "" ""

