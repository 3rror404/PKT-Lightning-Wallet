#!/bin/bash

#
# This script should be run from the project root
# e.g. ./contrib/gem/build.sh
#
fpm --prefix /usr/local -n pktd-mac -s dir -t osxpkg -v "$(./bin/pld --version | sed 's/.* version //' | tr -d '\n')" ./bin