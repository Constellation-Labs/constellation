Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/trusty64"

  config.vm.hostname = 'constellation'

  config.vm.network "private_network", type: "dhcp"
  config.vm.network "forwarded_port", guest: 8080, host: 8080
  config.vm.network "forwarded_port", guest: 8001, host: 8001

  project_root = File.dirname(__FILE__)

  config.vm.synced_folder project_root, "/home/vagrant/constellation", type: "nfs"

  config.ssh.forward_agent = true

  config.vm.provider "virtualbox" do |vb|
     vb.memory = "2048"
  end

  # run minikube installer script
  config.vm.provision :shell, path: "minikube-installer.sh"

end
