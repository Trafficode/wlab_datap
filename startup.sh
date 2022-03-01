#!/bin/bash

#   ------------------------------------------------------------------------- /
#   wlab_datap: startup.sh
#   Created on: 23 feb 2020
#   Author: Trafficode
#   ------------------------------------------------------------------------- /

cd /home/wlab/weatherlab/wlabapp/wlab_datap/

LOG_FILE=/var/log/wlab_datap.log

log ()	{
	echo $(date) - 'startup.sh' - $1 >> $LOG_FILE
}


log ' '
log ' '
log 'wlab_datap start from: '$(pwd)

while [ true ]; do
	python main.py
	var=$?
	log "[*] exit code: "$var
	
	if test $var -eq 10
		then
    	# Reboot process.
    	log "[*] reboot application."
    elif test $var -eq 11
    then
    	log "[*] exit from application."
    	break
	elif test $var -eq 20
	then
    	log "[*] reboot device."
    	sudo reboot
	else
    	# Unhandled exception.
    	log "[*] unhandled exception."
	fi
	log "sleep 10"
	sleep 10
done

log 'exit from wlab_datap ... '

#   ------------------------------------------------------------------------- /
#    end of file
#   ------------------------------------------------------------------------- /
