package com.linch.bigdata.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;

public class CuratorApplication {
    private static final String ZK_SERVER = "node01:2181,node02:2181,node03:2181";

    public static void main(String[] args) {
        RetryPolicy retryPolicy = new RetryNTimes(3, 2000);
        CuratorClientUtil curatorClientUtil = null;
        try{
            curatorClientUtil =
                    CuratorClientUtil.initialize(ZK_SERVER, retryPolicy);
            curatorClientUtil
            .queryZNodeChildren("/")
            .watchZNode("/test")
            .createZNode("/test", "hello zk", CreateMode.PERSISTENT)
            .queryZNodeData("/test")
            .queryZNodeChildren("/")
            .setZNodeData("/test", "hello again")
            .queryZNodeData("/test")
            .createZNode("/test/data1", "hello data1", CreateMode.PERSISTENT)
            .deleteZNode("/test/data1");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(curatorClientUtil != null){
                curatorClientUtil.close();
            }
        }
    }
}
