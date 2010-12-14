#/bin/sh
#the "init" script to start and stop p2p transfers manager
#BINLOCATION="/usr/local/hyves/bin/p2p/"
BINLOCATION=/usr/local/bin/dtrack

trap DIE 2 9 15 

DIE(){
    #kill some processes, do cleanup etc.
    #kill `ps ajxf | grep /usr/local/bin/dtrack | grep -v grep | awk '{print $2}'`

    kill $listenerPID
    kill $ratePID
    [[ $bwManPID != '' ]] && kill $bwManPID

    echo "`date +%T.%N` ***p2p: STOP" >>$sessionDir/p2p_init.log
    exit 
}

#rtPORT=10001
#ovrPORT=10002
#rdPORT=6379
#P2Pmain=p2p-bootserver

sessionDir="/etc/p2p"

#myIP=$1        #IP or hostname
#myRack=$2
#myDC=$3

echo $$ >$sessionDir/dtrack.pid

#some logs, just to know what's going on:
echo "`date +%T.%N` ***p2p: START" >>$sessionDir/p2p_init.log
echo myIP is $myIP >>$sessionDir/p2p_init.log
echo myHOST is $myHOST >>$sessionDir/p2p_init.log
echo myChannel is $myChannel >>$sessionDir/p2p_init.log
echo myRack is $myRack >>$sessionDir/p2p_init.log
echo myDC is $myDC >>$sessionDir/p2p_init.log
echo rtPORT is $rtPORT >>$sessionDir/p2p_init.log
echo rdPORT is $rdPORT >>$sessionDir/p2p_init.log
echo ovrPORT is $ovrPORT >>$sessionDir/p2p_init.log
echo p2pMAIN is $p2pMAIN >>$sessionDir/p2p_init.log

if [[ $myIP == '' ]] || [[ $myHOST == '' ]] || [[ $myChannel == '' ]] || [[ $myRack == '' ]] || [[ $myDC == '' ]] || [[ $rtPORT == '' ]] || [[ $rdPORT == '' ]] || [[ $ovrPORT == '' ]] || [[ $p2pMAIN == '' ]] ;
    then
    echo "One of the parameters is empty!"
    exit 255
fi

cp $BINLOCATION/xmlrpc2scgi.py $sessionDir
cp $BINLOCATION/throttles.sh $sessionDir
cp $BINLOCATION/die.sh $sessionDir
cp $BINLOCATION/finished.py $sessionDir

#the main overlay manager
$BINLOCATION/listener.py $myIP $myHOST $myChannel $myRack $myDC $rtPORT $ovrPORT $p2pMAIN $rdPORT >>$sessionDir/_listener.log 2>>$sessionDir/_listener.err &
listenerPID=$!

#only the main BW manager:
if [[ $myIP == $p2pMAIN ]] ; then
    $BINLOCATION/BW_manager.py $BINLOCATION/ports .emerge.channel >>$sessionDir/emerge_speeds.log 2>&1 &
    bwManPID=$! 
fi

#TODO: what do I need rtPID for except manage_local_rate???
rtPID=`$BINLOCATION/xmlrpc2scgi.py -p /etc/p2p/SCGI.socket system.pid`
#tPID=`ps ajxf | grep /bin/rtorrent | grep -v grep | grep -v rtorrentd | awk '{print $2}'`
echo rtPID is $rtPID >>$sessionDir/p2p_init.log

#every 10s check local speeds and adjust throttles
$BINLOCATION/manage_local_rate.sh $rtPID $myIP 10 $sessionDir >>$sessionDir/manage_link_speeds.log 2>&1 &
ratePID=$!

#sleep forewer
while [[ 1 -eq 1 ]] ; do
    sleep 1
done

#cd $startDir
