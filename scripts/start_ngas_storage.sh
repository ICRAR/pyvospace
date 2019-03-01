#!/usr/bin/env bash

logfile=$PYVOSPACE_DIR/scripts/log/ngas_storage.log
cd $PYVOSPACE_DIR
ngas_storage --cfg test_vo_ngas.ini 2>&1 | tee $logfile

