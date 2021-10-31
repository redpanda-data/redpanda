# docker build context
rm -fr /tmp/build/redpanda
mkdir /tmp/build
mkdir /tmp/build/redpanda
cp -r ../../redpanda/* /tmp/build/redpanda
rm -fr /tmp/build/redpanda/.git

sudo docker build -t redpanda.community.build -f Dockerfile /tmp/build

