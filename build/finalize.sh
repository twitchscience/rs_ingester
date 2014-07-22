#!/bin/bash --
set -e -u -o pipefail

BASEDIR="/opt/science"
INGESTERDIR="${BASEDIR}/redshiftIngester"
CONFDIR="${INGESTERDIR}/config"
UPSTARTDIR="/etc/init"
PKGDIR="/tmp/pkg"

mv ${PKGDIR}/deploy ${INGESTERDIR}
chmod +x ${INGESTERDIR}/bin/*

# Setup upstart
mv ${CONFDIR}/upstart.conf ${UPSTARTDIR}/redshiftIngester.conf
