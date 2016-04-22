#! /usr/bin/env bash

set -e

sudo apt-get update
sudo apt-get install -y tar curl git

# Libraries required to build a complete python with pyenv:
# https://github.com/yyuu/pyenv/wiki
sudo apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev \
    libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev

# Download and install the NSQ binaries
export NSQ_VERSION=0.2.26
export NSQ_DIST="nsq-${NSQ_VERSION}.linux-amd64.go1.2"
pushd /tmp
    wget "https://s3.amazonaws.com/bitly-downloads/nsq/${NSQ_DIST}.tar.gz"
    tar xf "${NSQ_DIST}.tar.gz"
    pushd ${NSQ_DIST}
        sudo cp bin/* /usr/local/bin/
    popd
popd

# Create an NSQ user
sudo useradd -U -s /bin/false -d /dev/null nsq
sudo mkdir /var/lib/nsqd
sudo chown nsq:nsq /var/lib/nsqd

# Copy all relevant files
sudo rsync -r /vagrant/scripts/vagrant/files/ /

# And start the relevant services
sudo service nsqlookupd start
sudo service nsqadmin start
sudo service nsqd start


# Start nsqlookupd and disown it
nsqlookupd > /dev/null 2> /dev/null &
disown

# And an instance of nsqd
nsqd --lookupd-tcp-address=127.0.0.1:4160 > /dev/null 2> /dev/null &

# Install pyenv
git clone https://github.com/yyuu/pyenv.git ~/.pyenv
echo '
# Pyenv
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
' >> ~/.bash_profile
source ~/.bash_profile
hash

pushd /vagrant
    # Install our python version
    pyenv install
    pyenv rehash

    # Install a virtualenv
    pip install virtualenv
    virtualenv venv
    source venv/bin/activate

    # Install our dependencies
    pip install -r requirements.txt 
popd
