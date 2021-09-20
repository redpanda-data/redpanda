---
title: Kubernetes快速入门指南
order: 0
---

# Kubernetes快速入门指南

Redpanda是用于关键任务业务的先进的[流式计算平台](/blog/intelligent-data-api/)。
借助Redpanda，您可以快速启动并运行流式传输
并与[Kafka生态系统](https://cwiki.apache.org/confluence/display/KAFKA/Ecosystem)完全兼容。

本快速入门指南可以帮助您开始使用Redpanda进行开发和测试。
要启动并运行，您需要创建一个cluster并将Redpanda operator部署在该cluster上。

- 对于生产或基准测试，请设置[生产部署](/docs/production-deployment)。
- 您还可以设置[具有外部访问权限的Kubernetes cluster](/docs/kubernetes-external-connect)。

> **_注意_** - 在Kubernetes cluster内部运行一个容器，以与Redpanda cluster进行通信。
> 当前，默认情况下，部署期间不会自动创建负载均衡器。

## 前置条件

在开始安装Redpanda之前，您需要设置Kubernetes环境。

### 安装Kubernetes，Helm和cert-manager

您需要安装：

- Kubernetes v1.16或更高版本
- [kubectl](https://kubernetes.io/docs/tasks/tools/) v1.16或更高版本
- [helm](https://github.com/helm/helm/releases) v3.0.0或更高版本
- [cert-manager](https://cert-manager.io/docs/installation/kubernetes/) v1.2.0或更高版本

请按照说明验证cert-manager已准备好创建证书。

### 创建一个Kubernetes cluster

您可以在本地或云上创建Kubernetes cluster。

[Kind](https://kind.sigs.k8s.io)是一种工具，可让您使用Docker创建本地Kubernetes cluster。
安装Kind之后，请使用以下步骤设置cluster：

```
kind create cluster
```

使用[EKS入门](https://docs.aws.amazon.com/eks/latest/userguide/getting-started-eksctl.html)指南来设置EKS。
完成后，将安装`eksctl`，以便您可以在EKS中创建和删除cluster。
然后，使用以下命令创建一个EKS cluster：

```
eksctl create cluster \
--name redpanda \
--nodegroup-name standard-workers \
--node-type m5.xlarge \
--nodes 1 \
--nodes-min 1 \
--nodes-max 4 \
--node-ami auto
```

该过程大约需要10-15分钟。

首先完成[Google Kubernetes Engine快速入门](https://cloud.google.com/kubernetes-engine/docs/quickstart)中描述的“开始之前”步骤。
然后，使用以下命令创建cluster：

```
gcloud container clusters create redpanda --machine-type n1-standard-4 --num-nodes=1
```

**_注意_** - 您可能需要在此命令中添加`--region`或`--zone`。

## 安装cert-manager

Redpanda operator需要cert-manager来创建用于TLS通信的证书。
您可以[使用CRD安装cert-manager](https://cert-manager.io/docs/installation/kubernetes/#installing-with-helm)，
但是这是使用helm安装的命令：

```
helm repo add jetstack https://charts.jetstack.io && \
helm repo update && \
helm install \
  cert-manager jetstack/cert-manager \
  --namespace cert-manager \
  --create-namespace \
  --version v1.2.0 \
  --set installCRDs=true
```

我们建议您使用cert-manager文档中的[验证过程](https://cert-manager.io/docs/installation/kubernetes/#verifying-the-installation)
来验证cert-manager是否正常工作。

## 使用Helm安装Redpanda

1. 使用Helm，添加Redpanda chart repository并进行更新：

    ```
    helm repo add redpanda https://charts.vectorized.io/ && \
    helm repo update
    ```

2. 为了简化命令，请为版本号创建一个变量：

    ```
    export VERSION=$(curl -s https://api.github.com/repos/vectorizedio/redpanda/releases/latest | jq -r .tag_name)
    ```

    **_注意_** - 您可以在[operator版本列表](https://github.com/vectorizedio/redpanda/releases)中找到operator的最新版本号。

3. 安装Redpanda operator CRD：

    ```
    kubectl apply \
    -k https://github.com/vectorizedio/redpanda/src/go/k8s/config/crd?ref=$VERSION
    ```

4. 使用以下命令在您的Kubernetes cluster上安装Redpanda operator：

    ```
    helm install \
    --namespace redpanda-system \
    --create-namespace redpanda-operator \
    --version $VERSION \
    redpanda/redpanda-operator
    ```

## 连接到Redpanda cluster

在Kubernetes cluster中配置Redpanda之后，您可以使用我们的示例安装cluster并查看运行中的Redpanda。

让我们尝试设置一个Redpanda topic，以处理具有5个聊天室的聊天应用程序中的事件流：

1. 为您的cluster创建一个namespace：

    ```
    kubectl create ns chat-with-me
    ```

2. 从[我们的样本文件](https://github.com/vectorizedio/redpanda/tree/dev/src/go/k8s/config/samples)安装cluster，例如单节点cluster：
                
    ```
    kubectl apply \
    -n chat-with-me \
    -f https://raw.githubusercontent.com/vectorizedio/redpanda/dev/src/go/k8s/config/samples/one_node_cluster.yaml
    ```

    您可以在[cluster_types文件](https://github.com/vectorizedio/redpanda/blob/dev/src/go/k8s/apis/redpanda/v1alpha1/cluster_types.go)中查看资源配置选项。

3.使用`rpk`来处理Redpanda节点，例如：

    a. 查看cluster的状态：

        kubectl -n chat-with-me run -ti --rm \
        --restart=Never \
        --image docker.vectorized.io/vectorized/redpanda:$VERSION \
        -- rpk --brokers one-node-cluster-0.one-node-cluster.chat-with-me.svc.cluster.local:9092 \
        cluster info
    
    b. 创建一个topic：

        kubectl -n chat-with-me run -ti --rm \
        --restart=Never \
        --image docker.vectorized.io/vectorized/redpanda:$VERSION \
        -- rpk --brokers one-node-cluster-0.one-node-cluster.chat-with-me.svc.cluster.local:9092 \
        topic create chat-rooms -p 5

    c. 显示topic列表：

        kubectl -n chat-with-me run -ti --rm \
        --restart=Never \
        --image docker.vectorized.io/vectorized/redpanda:$VERSION \
        -- rpk --brokers one-node-cluster-0.one-node-cluster.chat-with-me.svc.cluster.local:9092 \
        topic list

如您所见，来自"rpk" pod的commands创建了一个5个分区的聊天室。

## 下一步

- 在我们的[Slack](https://vectorized.io/slack)社区中与我们联系，以便我们可以共同实施您的Kubernetes use cases。
- 了解如何使用[从外部计算机访问](/docs/kubernetes-external-connect)设置Kubernetes cluster。
