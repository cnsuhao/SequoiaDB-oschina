#!/bin/bash
echo "###############building file..."
./buildApp.sh connect
./buildApp.sh snap
./buildApp.sh update
./buildApp.sh insert
./buildApp.sh query
./buildApp.sh sampledb
./buildApp.sh sql
./buildApp.sh update_use_id
./buildApp.sh index
./buildApp.sh subArrayLen
./buildApp.sh upsert
./buildApp.sh lob

echo "###############running connect..."
./build/connect localhost 11810 "" ""
echo "###############running snap..."
./build/snap localhost 11810 "" ""
echo "###############running update..."
./build/update localhost 11810 "" ""
echo "###############running insert..."
./build/insert localhost 11810 "" ""
echo "###############running query..."
./build/query localhost 11810 "" ""
echo "###############running sampledb..."
./build/sampledb localhost 11810 "" ""
echo "###############running sql..."
./build/sql localhost 11810 "" ""
echo "###############running update_use..."
./build/update_use_id localhost 11810 "" ""
echo "###############running index..."
./build/index localhost 11810 "" ""
echo "###############running subArrayLen..."
./build/subArrayLen localhost 11810 "" ""
echo "###############running upsert..."
./build/upsert localhost 11810 "" ""
echo "###############running lob..."
./build/lob localhost 11810 "" ""

echo "###############running connect.static..."
./build/connect.static localhost 11810 "" ""
echo "###############running snap.static..."
./build/snap.static localhost 11810 "" ""
echo "###############running update.static..."
./build/update.static localhost 11810 "" ""
echo "###############running insert.static..."
./build/insert.static localhost 11810 "" ""
echo "###############running query.static..."
./build/query.static localhost 11810 "" ""
echo "###############running sampledb.static..."
./build/sampledb.static localhost 11810 "" ""
echo "###############running sql.static..."
./build/sql.static localhost 11810 "" ""
echo "###############running update_use_id.static..."
./build/update_use_id.static localhost 11810 "" ""
echo "###############running index.static..."
./build/index.static localhost 11810 "" ""
echo "###############running subArrayLen.static..."
./build/subArrayLen.static localhost 11810 "" ""
echo "###############running upsert.static..."
./build/upsert.static localhost 11810 "" ""
echo "###############running lob.static..."
./build/lob.static localhost 11810 "" ""

