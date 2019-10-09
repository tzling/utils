package com.tzl.util;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.Charset;
import java.util.concurrent.*;

public class ZookeeperUtil {

    private CuratorFramework client;

    ///设置临时节点
    public void setTemporaryNode(String nodeName) throws Exception {
        this.setNode(nodeName, CreateMode.EPHEMERAL);
    }

    ///设置临时节点与参数
    public void setTemporaryNode(String nodeName, String value) throws Exception {
        this.setNode(nodeName, value, CreateMode.EPHEMERAL);
    }

    ///设置持久节点
    public void setNode(String nodeName) throws Exception {
        this.setNode(nodeName, CreateMode.PERSISTENT);
    }

    ///设置持久节点与参数
    public void setNode(String nodeName, String value) throws Exception {
        this.setNode(nodeName, value, CreateMode.PERSISTENT);
    }

    ///删除节点
    public void deleteNode(String nodeName) throws Exception {
        client.delete().guaranteed().deletingChildrenIfNeeded().forPath(nodeName);
    }

    ///判断节点是否存在
    public boolean hasNodeExits(String nodeName) throws Exception {
        Stat result = client.checkExists().forPath(nodeName);
        return result != null;
    }

    ///更新节点值
    public void updateNodeValue(String nodeName, String value) throws Exception {
        client.setData().forPath(nodeName, value.getBytes());
    }

    private final ExecutorService executors = Executors.newFixedThreadPool(8);
    private final ConcurrentMap<String,InterProcessMutex> interProcessMutex = new ConcurrentHashMap<String, InterProcessMutex>();

    ///节点同步
    public <T extends Number> Future<T>  synchronizedNode(String nodeName, Callable<T> callable) throws Exception {
        InterProcessMutex currentInterProcessMutex = interProcessMutex.get(nodeName);
        if(currentInterProcessMutex == null){
            synchronized (interProcessMutex){
                currentInterProcessMutex = interProcessMutex.get(nodeName);
                if(currentInterProcessMutex == null){
                    currentInterProcessMutex = new InterProcessMutex(client, nodeName);
                    interProcessMutex.put(nodeName,currentInterProcessMutex);
                }
            }
        }
        if(currentInterProcessMutex.acquire(3, TimeUnit.SECONDS)){
            try {
                return executors.submit(callable);
            } finally{
                currentInterProcessMutex.release();
            }
        }
        return null;
    }

    ///获取节点值
    public String getNodeValue(String nodeName) throws Exception {
        if (hasNodeExits(nodeName)) {
            byte[] value = client.getData().forPath(nodeName);
            return new String(value, Charset.forName("UTF-8"));
        }
        return null;
    }

    ///设置持久节点与参数
    public void setNodeVersion(String nodeName) throws Exception {
        client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(nodeName);
    }


    ///设置节点
    private void setNode(String nodeName, CreateMode modeType) throws Exception {
        client.create().creatingParentContainersIfNeeded().withMode(modeType).forPath(nodeName);
    }

    ///设置节点与参数
    private void setNode(String nodeName, String value, CreateMode modeType) throws Exception {
        client.create().creatingParentContainersIfNeeded().withMode(modeType).forPath(nodeName, value.getBytes());
    }

    ///创建连接
    public void start(String connectAddress, String namespace) {
        CuratorFramework client = createConnect(connectAddress, namespace).build();
        client.start();
        this.client = client;
    }

    ///连接对象
    private CuratorFrameworkFactory.Builder createConnect(String connectAddress) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        return CuratorFrameworkFactory.builder()
                .connectString(connectAddress)
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(3000)
                .retryPolicy(retryPolicy);
    }

    ///连接对象
    private CuratorFrameworkFactory.Builder createConnect(String connectAddress, String namespace) {
        CuratorFrameworkFactory.Builder createConnect = createConnect(connectAddress);
        createConnect.namespace(namespace);
        return createConnect;
    }


}
