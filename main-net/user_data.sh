#!/usr/bin/env bash
mkdir -p --mode 0700 /home/admin/.ssh
touch /home/admin/.ssh/authorized_keys
echo "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDb6+HQ2Q7zF2gchJZY0Ys1Q9q38xn8h/XJXFfxPyREeMuHr3lialiksDkfeXFtDl6DIzuiabCeFH8fJjfErHFZL82V9QiXmXqBov60DuTl1yWm8S2PN1Nf8ytoMnElqfOx+6Trk4QnbBVbgWt/d3BiUr3rZPSxEjgJjSTsEvbQSBqytwIWZIZF380wy1520rvRb6DsL/JfMDKcyh/NKtwa+P8gaIlSR0OqRJD2zDLr4S1x0SaAYY4cfJ0XohjqqcYSJ9MWD9RblOsi91SdO6rKrAG0gRKROn57oCicUYiitq38O6X5BqoEoV0201b4cwIo6VzkYSyI8DgFD3cwn5aJ mwadon" >> /home/admin/.ssh/authorized_keys
echo "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQCoaQ4GycMaxDjebYpWBEBZGMYE5NmXLQnDaYr08GKYZ0ogzrNdEM7r3ct0WXQnRTvtwSUfz2UYMeot0HlyZSumdQRPCIscBcNfghfjyfXE9cjOcI1ORXyGB456z5JS/tpfwuQxaMTqKqRijIiMURQIFkn0AeDl+hTby+Bocdw76PMRJJVxy3oGheOztWKkA2bTCVe7to1euYhRHt2L1wNMqZ8wHtjEXCBRRYCPm44+qXDWn4zpOsTz1GZj5cV/HOhK4rUaOkid9tkczShwVH9rCME6bS52E+q5CJWtVUbctFTkZWEKNSd5NCVLTgz+aG/ufg/tr+aXHE1BnmayBrArzpHFZlEGVHc3+P807sd83Xy5EOxeorca9DYHOjsqlIsnuZPe6VJ5rhfTOfKFwYlCadRkKR1zYuJJkKHg9MU8TdELx4tFB+I4PlUQq7n9kFC2NNZ4ejsWJN7MY0/yDlksTJJlTI08B2m34umVRYozw7geV3tKvb3+b1zjUuyrCBaKzMVk6qubVdwrPGhH9XURMtPic/Z5QqmDdw2Ukbd40UFd46x2TaJN69dN4YkK+oRfl3oNibC2QaI9563lOdGQ6Ffgj+kyAJad7lUD2Nz95jYsdG6Cp/2nxqRAWN4sjvp6u5MPTlFI+e095opZaR6Q7p2EUX59NalPzB6FQr4JeQ== kpudlik" >> /home/admin/.ssh/authorized_keys

chown admin:admin /home/admin/.ssh/authorized_keys
chmod 0600 /home/admin/.ssh/authorized_keys

# Disable auto-updates which were locking the apt lockfile
sudo sed -i 's/\(Unattended-Upgrade "\).*\(";\)/\10\2/g' /etc/apt/apt.conf.d/20auto-upgrades
cat /etc/apt/apt.conf.d/20auto-upgrades
sudo apt-get install -y jq
