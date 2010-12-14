#!/bin/sh
#p2p transfer of a file/dir
#DTRACK_BIN=/usr/local/dtrack

#TODO: trap ctrl+c and remove sessiondir, or even abourt the transfer

DTRACK_BIN=/tftp/root/p2p
    
    green='\e[0;32m'
    red='\e[0;31m'
    blue='\e[0;34m'
    yellow='\e[0;33m'
    nc='\e[0m' # No Color


function print_output() {
    type=$1
    text=$2

    if [[ ${type} == "error" ]];then
        echo -e "${red}(error) ${nc}${text}"
        exit 1;
    elif [[ ${type} == "info" ]];then
        echo -e "${nc}${text}"
    elif [[ ${type} == "warning" ]]; then
        echo -e "${red}!!! ${text} ${red}!!!${nc}"
    fi
}

function usage() {
    print_output info "Transfer of a file/directory to multiple hosts using p2p framework"
    print_output info ""
    print_output warning "EXISTING FILES AT DESTINATION WILL BE OVERWRITTEN"
    print_output info ""
    print_output info "Usage: ${0} -s <source> -d <destination_path> -t <targets_file> [-p <priority>]  [-x <speed>] "
    print_output info ""
    print_output info "Example: p2ptr.sh -s /tmp/my_dir -d /tmp/other_dir -t hosts -p 1 -x 300"
    print_output info ""
    print_output info "<source>              : path to the file or directory you want to transfer"
    print_output info ""
    print_output info "<destination_path>    : full path of the location on the destination host,"
    print_output info "                        where you want to transfer the file/dir to"
    print_output info ""
    print_output info "<priority>            : 1 | 2 | 3  <==> low, normal, high ; default is low"
    print_output info ""
    print_output info "<speed>               : speed in mbits assigned to this transfer; in case"
    print_output info "                        of multiple transfers it will be shared with other transfers,"
    print_output info "                        use then priorities to influence the real speed allocation;"
    print_output info "                        default speed is 300 mbits (~30MB/s)"
    print_output info ""
    print_output info "<targets_file>        : a file containing one destination host per line, "
    print_output info "                        e.g. systemnames -f dtrack >targets_file"
    exit
}

function err(){
    print_output error "$1"
    exit 255
}

# Parse commandline options
while getopts "d:hp:s:t:x:" options
do
    case ${options} in
        d ) DESTINATION="${OPTARG}";;
        h ) usage;;
        p ) PRIORITY="${OPTARG}";;
        s ) SOURCE="${OPTARG}";;
        t ) TARGETS="${OPTARG}";;
        x ) SPEED="${OPTARG}";;
        \? ) usage;;
        * ) usage;;
    esac
done

#Check if the parameters given are OK
[[ "$1" == "help" ]] && usage
[[ -z $1 ]] && usage
[[ -z ${DESTINATION} ]] && err "Destination is missing"
[[ `echo ${DESTINATION} | sed -n "/^\//p"` == '' ]] && err "Destination doesn't begin with / - you have to specify a full path"
[[ -z ${SOURCE} ]] && err "Source is missing"
[[ -z ${PRIORITY} ]] && PRIORITY=1
[[ "$PRIORITY" != "1" && "$PRIORITY" != "2" && "$PRIORITY" != "3" ]] && err "Priority has to be either unspecified or 1 or 2 or 3"
[[ "${SPEED}" == "" ]] && SPEED=300
[[ ! -f ${TARGETS} ]] && err "File with targets doesn't exist"
[[ ! -d ${SOURCE} && ! -f ${SOURCE} ]] && err "Source is not a file or directory"


name=`echo ${SOURCE} | sed "s|.\+/\([^/]\+\)|\1|"`
name=`echo $name | sed "s|/$||"`
current=`pwd`

if [[ `echo $SOURCE | sed -n "/^\//p"` != '' ]] ; then
    absSRC=$SOURCE
else
    absSRC=`readlink -e $current/$SOURCE`
fi

if [[ -d $absSRC ]] ; then
    dst=$DESTINATION/$name
    sDir=$absSRC/..
else
    dst=${DESTINATION}/
    sDir=`echo $absSRC | sed "s|\(.*\)/[^/]\+$|\1|"`
fi

echo "`date +%R:%S.%N` *** p2p START ***"
start_time=`date +%s.%N`

echo -e "Transferring ${green}${name}${nc} from ${green}${absSRC}${nc} to ${green}${dst}${nc} on target hosts, speed=${green}${SPEED}${nc} mbits, priority=${green}${PRIORITY}${nc}"
#echo name = $name
#echo absSRC = $absSRC
#echo dst = $dst
#echo sDir = $sDir

print_output info ""
print_output warning "EXISTING FILES AT DESTINATION WILL BE OVERWRITTEN"
print_output info ""

#make session dir
sessionDir="/tmp/p2p.init.$RANDOM.$RANDOM"
mkdir -p $sessionDir

#make the torrent file
mktorrent -a no_central_tracker -t 4 $absSRC -o $sessionDir/$name.torrent
cp $sessionDir/$name.torrent $sessionDir/${name}_FAST.torrent

#make _FAST torrent file
cd $sDir
HASH=`/usr/local/hyves/bin/p2p/rtorrent_fast_resume.pl <$sessionDir/${name}_FAST.torrent $sessionDir/${name}_FAST.torrent`
echo
echo -e "*** Hash is ${yellow}$HASH ${nc}***"
echo
cd $current

[[ ${HASH} == "" ]] && err "Hash is empty, aborting... (check parameters; is the file/directory empty?)"

#make md5 file
print_output info "Computing md5 sum..."
md5sum `find $absSRC -type f` | sed "s|$absSRC|/$DESTINATION/$name|" >$sessionDir/$HASH.md5 2>/dev/null

#stop the transfer if it might have been running
$DTRACK_BIN/p2pstop.sh ${HASH} >/dev/null


#I want to be the owner of the transfer
if [[ "`redis-cli2 -h $myHOST -p $rdPORT -b get ..${HASH}`" != "" ]] ; then
    redis-cli2 -h $myHOST -p $rdPORT -b set ..${HASH} $$ >/dev/null
    echo "The transfer is still running, waiting until it stops..."
    echo "(stop command already sent, just waiting for results)..."
    #wait till other stops = wait until ..{$HASH} key will get removed upon other script's finishing cleanup
    #but at most 5 seconds
    redis-cli2 -p $rdPORT -b -h $myHOST expire ..${HASH} 5 >/dev/null
    while true ; do
        pid="`redis-cli2 -h $myHOST -p $rdPORT -b get ..${HASH}`"
        [[ "${pid}" == "" ]] && break
        sleep 1
    done
fi

#now I got 30 seconds to start the transfer...
#redis-cli2 -p $rdPORT -b -h $myHOST expire ..${HASH} 30 >/dev/null

#I am the owner again...
redis-cli2 -h $myHOST -p $rdPORT -b set ..${HASH} $$ >/dev/null
#echo `date +%R:%S.%N` I am free to go 

#add targets to PENDING list
for a in `cat $TARGETS | awk '{print \$1}'` ; do
    redis-cli2 -h $myHOST -p $rdPORT -b sadd ${HASH}.PENDING $a >/dev/null
done

redis-cli2 -h $p2pMAIN -p $rdPORT -b sadd ${HASH}.INIT $myHOST >/dev/null
redis-cli2 -h $p2pMAIN -p $rdPORT -b sadd ${HASH}.SPEED ${SPEED} >/dev/null
redis-cli2 -h $p2pMAIN -p $rdPORT -b sadd ${HASH}.PRIORITY ${PRIORITY} >/dev/null

if [[ -f $absSRC ]] ; then
    absSRC=`echo $absSRC | sed "s|/[^/]\+$|/|"`
fi

#start the init seeder
echo -n "dtrack: "
sudo $DTRACK_BIN/sockreply.py /etc/p2p/MGMT.socket "INIT $HASH $myHOST $absSRC $sessionDir/${name} $sessionDir/$HASH.md5 $SPEED $dst $PRIORITY"

#print some statistics
echo -e "To see on-line stats, use ${green}watch $DTRACK_BIN/trstats.sh $HASH $myHOST ${nc}"
echo "Now waiting for transfer to finish, list of done hosts (corrupt hosts >stderr):"
#wait until it's all done
#/usr/local/hyves/bin/p2p/check_transfer.sh $HASH l $myHOST
echo

while true ; do
    newFIN=`redis-cli2 -p $rdPORT -b -h $myHOST sdiff ${HASH}.FINISHED .${HASH}.doneFIN`
    newCOR=`redis-cli2 -p $rdPORT -b -h $myHOST sdiff ${HASH}.CORRUPT .${HASH}.doneCOR`
    [[ "${newFIN}" != "" ]] && echo "${newFIN}" | tr ' ' '\n'
    [[ "${newCOR}" != "" ]] && echo "${newCOR}" | tr ' ' '\n' >&2
    for a in $newFIN ; do
         redis-cli2 -p $rdPORT -b -h $myHOST sadd .${HASH}.doneFIN $a >/dev/null
    done
    for a in $newCOR ; do
         redis-cli2 -p $rdPORT -b -h $myHOST sadd .${HASH}.doneCOR $a >/dev/null
    done
    [[ "`redis-cli2 -p $rdPORT -b -h $myHOST scard ${HASH}.PENDING`" == "0" ]] && break
    if [[ "`redis-cli2 -p $rdPORT -b -h $myHOST get ..${HASH}`" != "$$" ]] ; then
        echo "Somebody else (re-)started the same transfer, quitting..."
        break
    fi
    redis-cli2 -p $rdPORT -b -h $myHOST set ..${HASH} $$ >/dev/null
    #redis-cli2 -p $rdPORT -b -h $myHOST expire ..${HASH} 5 >/dev/null
    sleep 0.2
done

newFIN=`redis-cli2 -p $rdPORT -b -h $myHOST sdiff .${HASH}.FINISHED .${HASH}.doneFIN`
newCOR=`redis-cli2 -p $rdPORT -b -h $myHOST sdiff .${HASH}.CORRUPT .${HASH}.doneCOR`
[[ "${newFIN}" != "" ]] && echo $newFIN | tr ' ' '\n'
[[ "${newCOR}" != "" ]] && echo $newCOR | tr ' ' '\n' >&2



echo "Corrupt hosts:"
redis-cli2 -p $rdPORT -b -h $myHOST smembers .${HASH}.CORRUPT 

#cleanup redis
#normal keys
#for a in `redis-cli2 -p $rdPORT -b -h $myHOST keys ${HASH}\*` ; do
#    redis-cli2 -p $rdPORT -b -h $myHOST del $a >/dev/null
#done

#special keys
#for a in `redis-cli2 -p $rdPORT -b -h $myHOST keys .${HASH}\*` ; do
#    redis-cli2 -p $rdPORT -b -h $myHOST del $a >/dev/null
#done

#and a finished trigger key
redis-cli2 -p $rdPORT -b -h $myHOST del ..${HASH} >/dev/null

#cleanup temp
rm -r $sessionDir

stop_time=`date +%s.%N`
total_time=`echo "$stop_time-$start_time" | bc` >/dev/null 2>/dev/null
echo "`date +%R:%S.%N` *** p2p FINISHED in $total_time sec ***"
