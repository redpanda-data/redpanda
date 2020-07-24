FROM centos:8

RUN dnf install -y jq npm
RUN npm install -g @apidevtools/swagger-cli
RUN mkdir -p /var/lib/pandaproxy

ADD pandaproxy.tar.gz /opt/pandaproxy
COPY entrypoint.sh /test/

WORKDIR /test
ENTRYPOINT ["./entrypoint.sh"]
