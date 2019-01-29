#!/usr/bin/env bash

logfile=$PYVOSPACE_DIR/scripts/log/ngas_space.log
cd $PYVOSPACE_DIR
ngas_space --cfg test_vo_ngas.ini 2>&1 | tee $logfile

