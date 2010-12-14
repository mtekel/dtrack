#this script sets maximal rates according to local link usage
#parameters: rtPID, hostname/address , looptime, sessionDir

#INIT:
#1. nslookup hostname, if it is not IP  [because transfers are internal, I DO expect nslookup returns only one result]
#2. find out which eth interface it is (ifconfig, grep address, look on 1 line before it, get the mac, then grep the mac against eth)
#3. find out speed - use ethtool (interface)

#LOOP:
#4. get average network card speeds: compute average yourself, cat /proc/net/dev etc.
#5. get rates from rtorrent
#6. new speed = linkspeed-used_speed-reserve+rate (e.g. 1000-750-200+300=350 (750mbit was used, out of this I was using 300, 200 is reserve))
#7. set new speed limits
#8. that's it, done, sleep for the rest of time, then loop again


ETHTOOL=/usr/sbin/ethtool

IPhost=$2
looptime=$3
sessionDir=$4
reserve=200 #mbit that is
modifier=1  #link speed modifier for the half duplex
cpu_mod=1   #cpu utilization modifier
cpu_tresh=0.3   #if cpu_idle gets below this value, the speed gets linearly scaled to whatever that's below it, e.g. 0.05 idle = 25% of the speed, 0.16 idle = 80% of the speed if cpu_tresh=0.2

#I suppose that the hostname or the ip address doesn't change during the transfer (during when this script is run)

#get the IP
if [ `echo $IPhost | grep "[0-9]\+\.[0-9]\+\.[0-9]\+\.[0-9]\+" | wc -l` -eq 0 ] ; then
    IP=`nslookup $IPhost | grep -A 1 Name: | grep Address: | sed "s:.\+\ \([0-9]\+\.[0-9]\+\.[0-9]\+\.[0-9]\+\):\1:"` ;
  else
    IP=$IPhost
fi

MAC=`ifconfig | grep $IP -B 1 | grep -o "HWaddr.\+$" | sed "s:HWaddr ::"`
eth=`ifconfig | grep -m 1 $MAC | grep -o "eth[0-9]\+"`      #take the first interface that matches the MAC


#if the link is not up, then I resolved the wrong IP? or how else can I get to a host with link down?
up=$(sudo ${ETHTOOL} ${eth} | sed -n -e '/Link detected: /s/^.*: \(.\+\)$/\1/p')
    if [[ ${up} == no ]] ; then
        echo "***resolved hostname/IP LINK DOWN!!!, something wrong here, check the script..." >>$sessionDir/manage_link_speeds.log ;
        exit;
    fi
#also the duplex should be full, if it is not, then there's maybe network configuration problem, nevertheless, I'll assign only 50% of speeds
duplex=$(sudo ${ETHTOOL} ${eth} | sed -n -e '/Duplex: /s/^.*: \(.\+\)$/\1/p')
    if [[ "${speed}" == "" && "${duplex}" == "" ]]; then
        echo "***resolved hostname/IP does not exist!!!, st. wrong here, check the script/situation" >>$sessionDir/manage_link_speeds.log ;
        exit;
    elif [[ "${speed}" == "Unknown" && "${duplex}" == "Half" ]]; then
        echo "***resolved hostname/IP duplex unknown or HALF!!!, st. wrong here, check the script/situation" >>$sessionDir/manage_link_speeds.log ;
        modifier="0.5"      #modifies both speeds to 50% - because I suppose the duplex is half
        #in fact, I shouldn't be using the modifier, but manage the link speed as cumulative for download and upload, but then - to which give higher priority? nevertheless, half duplex link speeds stink and it quite well might be also 10mbit
    fi

speed=`sudo ${ETHTOOL} $eth | sed -n -e '/Speed: /s/^.*: \(.\+\)Mb\/s$/\1/p' | sed -e 's/!.*//g'`
linkspeed=`echo "($speed - $reserve) * $modifier / 10 * 1024 * 1024   "| bc`     #count in reserve and convert to B/s (/10 is to give some overhead reserve)

interval=`echo $looptime - 1 | bc`

echo speed is $speed >>$sessionDir/manage_link_speeds.log
echo IP is $IP >>$sessionDir/manage_link_speeds.log
echo linkspeed is $linkspeed >>$sessionDir/manage_link_speeds.log
echo eth is $eth >>$sessionDir/manage_link_speeds.log
echo looptime is $looptime >>$sessionDir/manage_link_speeds.log


#TODO: add cpu feedback to the loop
while [ $(ps $1 | wc -l) -eq 2 ]
do
    old_time=`date +%s.%N`

    down1=`cat /proc/net/dev | grep $eth | sed 's/:/ /g;s/\ \+/ /g;s/^\ //' | cut -d " " -f 2`
    up1=`cat /proc/net/dev | grep $eth | sed 's/:/ /g;s/\ \+/ /g;s/^\ //' | cut -d " " -f 10`

#    sleep $interval
#mpstats takes $interval time to report average cpu usage over that interval, thus no need to sleep

    cpu=`mpstat $interval 1 | awk '/Average/ {print $10}'`
    cpu_idle=`echo "scale=3; $cpu / 100"  | bc`

    down2=`cat /proc/net/dev | grep $eth | sed 's/:/ /g;s/\ \+/ /g;s/^\ //' | cut -d " " -f 2`
    up2=`cat /proc/net/dev | grep $eth | sed 's/:/ /g;s/\ \+/ /g;s/^\ //' | cut -d " " -f 10`

    #actually, the real interval is longer, because it takes some time to process all the commands, thus I compute a bit higher average here - but this just adds to the "bandwidth safety"
    used_down=`echo " ($down2 - $down1) / $interval " | bc`
    used_up=`echo " ($up2 - $up1) / $interval " | bc`

    cpu_mod=1
    [[ `echo "$cpu_idle < $cpu_tresh" | bc` -eq 1 ]] && cpu_mod=`echo "scale=3; 1 - ( $cpu_tresh - $cpu_idle ) * ( 1 / $cpu_tresh )" | bc`      #if the cpu util is above 80%, speed will be proportional to the remaining utilization to 100%, e.g. 84% cpu util = 80% speed, 95% util = 25% speed

#!!!get rate of the download - it is more representing (OR! adjust global rate to be more representative, e.g. 10s interval)
#these rates are in B/s
    down=`$sessionDir/xmlrpc2scgi.py -p $sessionDir/SCGI.socket get_down_rate 2>/dev/null`
    up=`$sessionDir/xmlrpc2scgi.py -p $sessionDir/SCGI.socket get_up_rate 2>/dev/null`

    new_down=`echo "$linkspeed - $used_down + $down" | bc 2>/dev/null`
    new_up=`echo "$linkspeed - $used_up + $up" | bc 2>/dev/null`

    if [[ $new_down -gt $linkspeed ]] ; then new_down=$linkspeed ; fi
    if [[ $new_up -gt $linkspeed ]] ; then new_up=$linkspeed ; fi

    new_down=`echo "scale=0; ( $new_down * $cpu_mod ) / 1 " | bc`
    new_up=`echo "scale=0; ( $new_up * $cpu_mod ) / 1" | bc`

#    echo "down1, down2, up1, up2, used_up, used_down, up, down = $down1, $down2, $up1, $up2, $used_up, $used_down, $up, $down "  >>$sessionDir/manage_link_speeds.log
    echo "`date +%R:%S.%N` about to set $new_down download rate and $new_up upload rate, cpu idle is $cpu_idle, modifier is $cpu_mod" >>$sessionDir/manage_link_speeds.log

#& because I can be already getting used speeds, I don't need to wait until rates are set...
    $sessionDir/xmlrpc2scgi.py -p "$sessionDir/SCGI.socket" set_download_rate $new_down 2>/dev/null >/dev/null &
    $sessionDir/xmlrpc2scgi.py -p "$sessionDir/SCGI.socket" set_upload_rate $new_up 2>/dev/null >/dev/null &


    new_time=`date +%s.%N`
    sleeptime=`echo "$looptime-($new_time-$old_time)" | bc`
#    echo "sleeptime = $sleeptime " >>$sessionDir/manage_link_speeds.log
    if [ `echo $sleeptime \<= 0 | bc` -eq 1 ] ; then echo "***manage_local_rate: looptime too short ($sleeptime), could not process all feedback and set according speed limits, possible bandwidth leak!" >>$sessionDir/manage_link_speeds.log; fi
    sleep $sleeptime >/dev/null 2>/dev/null
done

