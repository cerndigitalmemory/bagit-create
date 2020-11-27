#!bin/sh

# Archivematica
## https://github.com/artefactual-labs/am/tree/master/compose#docker-and-linux

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

# Enduro
## https://enduroproject.netlify.app/docs/development/environment/

curl --silent --location https://dl.yarnpkg.com/rpm/yarn.repo | sudo tee /etc/yum.repos.d/yarn.repo
curl --silent --location https://rpm.nodesource.com/setup_12.x | sudo bash -
yum install -y yarn

yum install go

docker-compose up --detach
make cadence-seed
make cadence-domain
make tools
make migrations ui
make
