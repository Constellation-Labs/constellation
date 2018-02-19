Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/trusty64"

  config.vm.hostname = 'constellation'

  config.vm.network "private_network", ip: "192.168.33.77"

  # update to point to your host constellation repo location
  config.vm.synced_folder "~/projects/constellationlabs/constellation", "/home/vagrant/constellation"

  config.ssh.forward_agent = true

  config.vm.provider "virtualbox" do |vb|
     vb.memory = "2048"
  end

  # run minikube installer script
  config.vm.provision :shell, path: "minikube-installer.sh"

end
