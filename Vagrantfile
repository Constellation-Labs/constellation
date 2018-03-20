Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/trusty64"

  config.vm.hostname = 'constellation'

  config.vm.network "private_network", type: "dhcp", ip: "192.168.50.4"
  config.vm.hostname = "constellation.local"

  project_root = File.dirname(__FILE__)

  config.vm.synced_folder project_root, "/home/vagrant/constellation", type: "nfs"

  config.ssh.forward_agent = true

  config.vm.provider "virtualbox" do |vb|
     vb.memory = "2048"
  end

  # install dependencies
  config.vm.provision :shell, path: "install-dependencies.sh"

end
