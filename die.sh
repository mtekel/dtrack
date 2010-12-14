#parameters: hash, Bytes DONE (downloaded)
#called from rtorrent when download completes, this script just starts shutdown.sh, so that the rtorrent can continue running
#echo "`date +%s.%N` $2" > ./$1.FINISHED
sudo ./finished.py ./MGMT.socket $1 >/dev/null 2>&1 &
exit 0
