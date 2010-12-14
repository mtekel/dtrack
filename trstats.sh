#!/bin/sh
hsh=$1
rHost=$2

function hostinfo(){
    echo -n $1
    echo -e "\t `redis-cli2 -p $rdPORT -b -h $rHost get $hsh.doneB.$1` Bytes done; \t \t `redis-cli2 -p $rdPORT -b -h $rHost get $hsh.speeds.$1` B/s"
}

to_go_real=`redis-cli2 -p $rdPORT -b -h $rHost scard $hsh.PENDING`
done_real=`redis-cli2 -p $rdPORT -b -h $rHost scard $hsh.FINISHED`
corrupt_real=`redis-cli2 -p $rdPORT -b -h $rHost scard $hsh.CORRUPT`

done_finished=`redis-cli2 -p $rdPORT -b -h $rHost scard .$hsh.FINISHED`
corrupt_finished=`redis-cli2 -p $rdPORT -b -h $rHost scard .$hsh.CORRUPT`


if [[ "${to_go_real}" == "0" ]] ; then
    echo "*** ALL FINISHED ***" ; break ;
    echo ; exit 0
fi

echo -n "Systems still to finish the transfer: $to_go_real"
    echo
    for a in `redis-cli2 -p $rdPORT -b -h $rHost smembers $hsh.PENDING` ; do hostinfo $a ; done
    echo

echo -n "Systems succesfully finished transfer: $done_real"
    echo
    for a in `redis-cli2 -p $rdPORT -b -h $rHost smembers $hsh.FINISHED`; do hostinfo $a ; done
    echo

echo -n "Systems finished with CORRUPT file: $corrupt_real"
    echo
    for a in `redis-cli2 -p $rdPORT -b -h $rHost smembers $hsh.CORRUPT` ; do hostinfo $a ; done
    echo
