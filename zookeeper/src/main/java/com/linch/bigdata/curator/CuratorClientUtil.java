package com.linch.bigdata.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.zookeeper.CreateMode;

import java.util.List;
import java.util.concurrent.*;

public class CuratorClientUtil {
    private CuratorFramework client = null;

    private CuratorClientUtil(String zkServer, RetryPolicy retryPolicy){
        client = CuratorFrameworkFactory.newClient(zkServer, retryPolicy);
        client.start();
        System.out.println("ZK client start successfully");
    }

    public static CuratorClientUtil initialize(String zkServer, RetryPolicy retryPolicy){
        return new CuratorClientUtil(zkServer, retryPolicy);
    }

    public CuratorClientUtil createZNode(String path, String data, CreateMode mode) throws Exception {
        client.create()
                .creatingParentsIfNeeded()
                .withMode(mode)
                .forPath(path, data.getBytes());
        System.out.println(mode.name() + " ZNode: " + path + " with data: " + data + " created.");
        return this;
    }

    public CuratorClientUtil queryZNodeData(String path) throws Exception{
        byte[] bytes = client.getData()
                .forPath(path);
        System.out.println("Get data in ZNode - " + path + ": " + new String(bytes));
        return this;
    }

    public CuratorClientUtil queryZNodeChildren(String path) throws Exception{
        List<String> subPaths = client.getChildren()
                .forPath(path);
        System.out.println("Children in " + path + ": ");
        for(String subPath : subPaths){
            System.out.println(subPath);
        }
        return this;
    }

    public CuratorClientUtil setZNodeData(String path, String newData) throws Exception{
        client.setData()
                .forPath(path, newData.getBytes());
        System.out.println("Set data in ZNode - " + path + " : " + newData);
        return this;
    }

    public CuratorClientUtil deleteZNode(String path) throws Exception {
        client.delete()
                .forPath(path);
        System.out.println("Delete ZNode: " + path);
        return this;
    }

    public CuratorClientUtil watchZNode(String path){
        TreeCache treeCache = new TreeCache(client, path);
        treeCache.getListenable().addListener((client, event) -> {
            ChildData data = event.getData();
            if(data != null){
                switch (event.getType()) {
                    case NODE_ADDED:
                    case NODE_UPDATED:
                    case NODE_REMOVED:
                        System.out.println(event.getType().name() + ": " + data.getPath() + "\tdata: " + new String(data.getData()));
                        break;
                    default:
                        break;
                }
            } else {
                System.out.println("Watch Znode - " + path + ", data is null!");
            }
        });

        LinkedBlockingDeque blockingDeque = new LinkedBlockingDeque(3);
        ThreadPoolExecutor watchZNodeExecutor =
                new ThreadPoolExecutor(1, 2, Integer.MAX_VALUE, TimeUnit.MILLISECONDS, blockingDeque);
        watchZNodeExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    treeCache.start();
                    Thread.sleep(Integer.MAX_VALUE);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    treeCache.close();
                }
            }
        });

        return this;
    }

    public void close(){
        client.close();
        client = null;
    }
}
