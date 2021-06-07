---
title: Docker快速入门指南
order: 0
---

# Docker快速入门指南

Redpanda是用于任务关键型工作负载的现代[流媒体平台](/blog/intelligent-data-api/)。
借助Redpanda，您可以快速启动并运行流式传输
并与[Kafka生态系统](https://cwiki.apache.org/confluence/display/KAFKA/Ecosystem)完全兼容。

这份快速入门指南可以帮助您开始使用Redpanda进行开发和测试。
对于生产或基准测试，请参考[生产部署](/docs/production-deployment)。

## 准备好cluster

要准备好进行流传输的cluster，请运行Redpanda的单个Docker容器或3个容器的cluster。

> **_注意_** - 您也可以使用[`rpk container`](/docs/guide-rpk-container)在容器中运行Redpanda，从而完全无需与Docker进行交互。

### 单节点cluster的单个命令

使用单节点cluster，您可以测试Redpanda的简单实现。

**_注意_**:

- `--overprovisioned` 用于适配docker资源限制。
- `--pull=always` 确保您始终使用最新版本。

```bash
docker run -d --pull=always --name=redpanda-1 --rm \
-p 9092:9092 \
vectorized/redpanda:latest \
start \
--overprovisioned \
--smp 1  \
--memory 1G \
--reserve-memory 0M \
--node-id 0 \
--check=false
```

您可以执行一些[简单的topic操作](#Do-some-streaming)来进行一些流式传输。
或者，您只需将[Kafka兼容客户端](/docs/faq/#What-clients-do-you-recommend-to-use-with-Redpanda)指向到 `127.0.0.1:9092` 就可以了。

### 设置3节点cluster

要测试cluster中节点之间的交互，请在cluster中设置具有3个容器的Docker网络。

#### 创建network和persistent volumes

首先，我们需要建立一个桥接网络，以便Redpanda实例可以相互通信
但仍允许Kafka API在localhost上可用。
我们还将创建persistent volumes，这些persistent volumes使Redpanda实例在重启期间保持状态。

```bash
docker network create -d bridge redpandanet && \
docker volume create redpanda1 && \
docker volume create redpanda2 && \
docker volume create redpanda3
```

#### 启动Redpanda节点

然后，我们需要启动Redpanda cluster的节点。

```bash
docker run -d \
--pull=always \
--name=redpanda-1 \
--hostname=redpanda-1 \
--net=redpandanet \
-p 8082:8082 \
-p 9092:9092 \
-v "redpanda1:/var/lib/redpanda/data" \
vectorized/redpanda start \
--smp 1  \
--memory 1G  \
--reserve-memory 0M \
--overprovisioned \
--node-id 0 \
--check=false \
--pandaproxy-addr 0.0.0.0:8082 \
--advertise-pandaproxy-addr 127.0.0.1:8082 \
--kafka-addr 0.0.0.0:9092 \
--advertise-kafka-addr 127.0.0.1:9092 \
--rpc-addr 0.0.0.0:33145 \
--advertise-rpc-addr redpanda-1:33145 &&

docker run -d \
--pull=always \
--name=redpanda-2 \
--hostname=redpanda-2 \
--net=redpandanet \
-p 9093:9093 \
-v "redpanda2:/var/lib/redpanda/data" \
vectorized/redpanda start \
--smp 1  \
--memory 1G  \
--reserve-memory 0M \
--overprovisioned \
--node-id 1 \
--seeds "redpanda-1:33145" \
--check=false \
--pandaproxy-addr 0.0.0.0:8083 \
--advertise-pandaproxy-addr 127.0.0.1:8083 \
--kafka-addr 0.0.0.0:9093 \
--advertise-kafka-addr 127.0.0.1:9093 \
--rpc-addr 0.0.0.0:33146 \
--advertise-rpc-addr redpanda-2:33146 &&

docker run -d \
--pull=always \
--name=redpanda-3 \
--hostname=redpanda-3 \
--net=redpandanet \
-p 9094:9094 \
-v "redpanda3:/var/lib/redpanda/data" \
vectorized/redpanda start \
--smp 1  \
--memory 1G  \
--reserve-memory 0M \
--overprovisioned \
--node-id 2 \
--seeds "redpanda-1:33145" \
--check=false \
--pandaproxy-addr 0.0.0.0:8084 \
--advertise-pandaproxy-addr 127.0.0.1:8084 \
--kafka-addr 0.0.0.0:9094 \
--advertise-kafka-addr 127.0.0.1:9094 \
--rpc-addr 0.0.0.0:33147 \
--advertise-rpc-addr redpanda-3:33147
```

现在，您可以在其中一个容器上运行`rpk`以与cluster进行交互：

```bash
docker exec -it redpanda-1 rpk cluster info
```

status命令的输出如下所示：

```bash
  Redpanda Cluster Status

  0 (127.0.0.1:9092)       (No partitions)
  1 (127.0.0.1:9093)       (No partitions)
  2 (127.0.0.1:9094)       (No partitions)
```

### 调出docker-compose文件

您可以使用docker-compose文件轻松尝试不同的docker配置参数。

1. 将此内容另存为`docker-compose.yml`：

    ```yaml
    version: '3.7'
    services:
    redpanda:
        entrypoint:
        - /usr/bin/rpk
        - redpanda
        - start
        - --smp
        - '1'
        - --reserve-memory
        - 0M
        - --overprovisioned
        - --node-id
        - '0'
        - --kafka-addr
        - PLAINTEXT://0.0.0.0:29092,OUTSIDE://0.0.0.0:9092
        - --advertise-kafka-addr
        - PLAINTEXT://redpanda:29092,OUTSIDE://localhost:9092
        # NOTE: Please use the latest version here!
        image: vectorized/redpanda:v21.4.13
        container_name: redpanda-1
        ports:
        - 9092:9092
        - 29092:29092
    ```

2. 在保存文件的目录中运行：

    ```bash
    docker-compose up -d
    ```

如果要更改参数，请编辑docker-compose文件并再次运行命令。

## 试一下streaming

以下是一些用于生成和使用流的示例命令：

1. 创建一个topic。我们将其称为"twitch_chat"：

    ```bash
    docker exec -it redpanda-1 \
    rpk topic create twitch_chat --brokers=localhost:9092
    ```

1. 向topic发送消息：

    ```bash
    docker exec -it redpanda-1 \
    rpk topic produce twitch_chat --brokers=localhost:9092
    ```

    在topic中键入文本，然后按`Ctrl+D`在消息之间进行分隔。

    按`Ctrl+C`退出produce命令。

1. 消费（或阅读）topic中的消息：

    ```bash
    docker exec -it redpanda-1 \
    rpk topic consume twitch_chat --brokers=localhost:9092
    ```
    
    每条消息都带有其metadata，如下所示：
    
    ```bash
    {
    "message": "How do you stream with Redpanda?\n",
    "partition": 0,
    "offset": 1,
    "timestamp": "2021-02-10T15:52:35.251+02:00"
    }
    ```

至此，您已经完成了Redpanda的安装，并通过几个简单的步骤完成了流式传输。 

## 清理

完成cluster操作后，可以使用以下方法关闭和删除容器：

```bash
docker stop redpanda-1 redpanda-2 redpanda-3 && \
docker rm redpanda-1 redpanda-2 redpanda-3
```

如果设置了volumes和network，请使用以下命令将其删除：

```bash
docker volume rm redpanda1 redpanda2 redpanda3 && \
docker network rm redpandanet
```

## 下一步

- 我们的[FAQ](/docs/faq)页面显示了可用于与Redpanda进行流媒体传输的所有客户端。
    （剧透：任何与Kafka兼容的客户端！）
- 使用[`rpk容器`](/docs/guide-rpk-container)启动并运行多节点cluster。
- 使用[快速入门Docker指南](/docs/quick-start-docker)基于Docker试用Redpanda。
- 是否要建立生产环境的cluster？请查看我们的[生产部署指南](/docs/production-deployment)。

<img src="https://static.scarf.sh/a.png?x-pxid=3c187215-e862-4b67-8057-45aa9a779055" />
