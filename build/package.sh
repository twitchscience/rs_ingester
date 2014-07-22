#!/bin/bash --
set -e -u -o pipefail

export SRCDIR="/home/vagrant/go/src/github.com/twitchscience/rs_ingester"
export PKGDIR="/tmp/pkg"
export DEPLOYDIR="${PKGDIR}/deploy"
export SUPPORTDIR="${PKGDIR}/support"

mkdir -p ${DEPLOYDIR}/{bin,data}
cp -R ${SRCDIR}/rs_ingester ${DEPLOYDIR}
cp ${SRCDIR}/build/scripts/* ${DEPLOYDIR}/bin
cp -R ${SRCDIR}/build/config ${DEPLOYDIR}
