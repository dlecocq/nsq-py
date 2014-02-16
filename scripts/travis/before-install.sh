#! /usr/bin/env bash

# Download and install the NSQ binaries
export NSQ_DIST="nsq-${NSQ_VERSION}.linux-$(go env GOARCH).go1.2"
wget "https://s3.amazonaws.com/bitly-downloads/nsq/${NSQ_DIST}.tar.gz"
tar xf "${NSQ_DIST}.tar.gz"
(
    cd "${NSQ_DIST}"
    sudo cp bin/* /usr/local/bin/
)

# Start nsqlookupd and disown it
nsqlookupd > /dev/null 2> /dev/null &
disown

# And an instance of nsqd
nsqd --lookupd-tcp-address=127.0.0.1:4160 > /dev/null 2> /dev/null &
disown
