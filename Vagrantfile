# Encoding: utf-8
# -*- mode: ruby -*-
# vi: set ft=ruby :

ENV['VAGRANT_DEFAULT_PROVIDER'] = 'virtualbox'

# http://docs.vagrantup.com/v2/
Vagrant.configure('2') do |config|
  config.vm.box = 'ubuntu/trusty64'
  config.vm.hostname = 'nsq-py'
  config.ssh.forward_agent = true

  config.vm.network 'forwarded_port', guest: 4150, host: 4150
  config.vm.network 'forwarded_port', guest: 4151, host: 4151
  config.vm.network 'forwarded_port', guest: 4161, host: 4161
  config.vm.network 'forwarded_port', guest: 4171, host: 4171

  config.vm.provider :virtualbox do |vb|
    vb.customize ["modifyvm", :id, "--memory", "1024"]
  end

  config.vm.provision :shell, path: 'scripts/vagrant/provision.sh', privileged: false
end
