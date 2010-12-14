for a in `redis-cli2 -h $myHOST -p $rdPORT -b keys $1.\*.\*.all` ; do
    redis-cli2 -h $myHOST -p $rdPORT -b publish $a.channel "*FINISHED transfer" >/dev/null
done
