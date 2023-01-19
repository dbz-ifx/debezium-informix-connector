#!/bin/bash

dbaccess < $INFORMIXDIR/etc/syscdcv1.sql

echo 'create database testdb with log;' | dbaccess - -
