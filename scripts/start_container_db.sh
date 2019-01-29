cd $PYVOSPACE_DIR/pyvospace/server/deploy
logfile=$PYVOSPACE_DIR/pyvospace/scripts/log/pyvospace_db.log

docker-compose up 2>&1 | tee $logfile
