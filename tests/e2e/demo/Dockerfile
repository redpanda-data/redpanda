FROM centos:8

RUN dnf install -y java-11-openjdk

RUN cd /opt && \
    curl -O https://apache.osuosl.org/kafka/2.4.1/kafka_2.12-2.4.1.tgz && \
    tar xzf kafka_2.12-2.4.1.tgz && \
    rm kafka_2.12-2.4.1.tgz && \
    ln -s /opt/kafka_2.12-2.4.1 /opt/kafka

COPY entrypoint.sh .
ENTRYPOINT ["./entrypoint.sh"]
