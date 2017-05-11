#!/bin/sh
# Docker entrypoint (pid 1), run as root

[ "$1" = "sh" ] && exec "$@"

webserver() {
	mkdir -p /www/tmp
	ln -sf /tmp/skale /www/tmp/skale
	httpd -h /www
}

trap 'echo terminated; kill $pid' SIGTERM

case $1 in
(skale-server|skale-worker)
	webserver
	log=/var/log/$1.log
	[ -f $log ] && mv $log $log.old
	cmd="cd; ulimit -c unlimited; env; echo $@; exec $@"
	su -s /bin/sh -c "$cmd" skale 2>&1 | tee /var/log/$1.log & pid=$!
	wait $pid
	;;
esac
