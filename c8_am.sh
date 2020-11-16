#!bin/sh

adduser avivace

yum install -y yum-utils git docker-compose
yum-config-manager \
    --add-repo \
    https://download.docker.com/linux/centos/docker-ce.repo
sudo sysctl -w 
vm.max_map_count=262144

echo "vm.max_map_count=262144" >> /etc/sysctl.conf

pip3 install docker-compose
systemctl start docker

git clone https://github.com/artefactual-labs/am.git
cd am
git submodule update --init --recursive

make create-volumes
docker-compose up -d --build

chmod o+r /etc/resolv.conf

firewall-cmd --zone=public --add-masquerade --permanent
firewall-cmd --zone=public --add-port=80/tcp
firewall-cmd --zone=public --add-port=443/tcp
firewall-cmd --reload

make bootstrap
make restart-am-services

