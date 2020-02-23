#!/bin/bash

#   ------------------------------------------------------------------------- /
#   wlab_datap: main.py
#   Created on: 29 sie 2019
#   Author: Trafficode
#   ------------------------------------------------------------------------- /

LOG_FILE=startup.log

log ()	{
	echo $(date) - 'startup.sh' - $1 >> $LOG_FILE
}

log ' '
log ' '
log 'wlab_web_dataprovider start from: '$(pwd)

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

log 'exit from wlab_web_dataprovider.sh ... '

# -----------------------------------------------------------------------------
#    end of file
# -----------------------------------------------------------------------------
