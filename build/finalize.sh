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
mv ${CONFDIR}/upstart/dbstorer.conf ${UPSTARTDIR}/dbstorer.conf
mv ${CONFDIR}/upstart/ingester.conf ${UPSTARTDIR}/ingester.conf
