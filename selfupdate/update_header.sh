#!/bin/bash

rw
touch /etc/FlightBox

# create the /etc/flightbox directory
if [ ! -d "/etc/flightbox" ]; then
  mkdir /etc/flightbox
fi

# create the /root/log directory
if [ ! -d "/root/log" ]; then
  rm /root/stratux.log
  mkdir /root/log
  mv /root/stratux.sql* /root/log/
fi

rm -rf /root/stratux-update
mkdir -p /root/stratux-update
cd /root/stratux-update
mv -f /root/log/stratux.sqlite /root/log/stratux.sqlite.`date +%s`
rm -f /root/log/stratux.sqlite-wal /root/log/stratux.sqlite-shm
