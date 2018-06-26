package com.tmg.internship.datacanal.producer;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * canal实例的配置信息，支持多实例，见application.properties
 * 以example实例为例，配置项如下：
 * canal.client.instances.example.host=127.0.0.1
 * canal.client.instances.example.port=11111
 * @author Paul
 * @company 天极云智
 * @date 2018/4/13
 **/
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
@ConfigurationProperties(prefix = "canal.client")
public class CanalConfig {

    /**
     * 是否启动canal客户端以获取canal事件
     */
    private boolean enabled;

    /**
     * 配置实例Map集合
     */
    private Map<String, Instance> instances = new LinkedHashMap<>();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Map<String, Instance> getInstances() {
        return instances;
    }

    public void setInstances(Map<String, Instance> instances) {
        this.instances = instances;
    }

    /**
     * 实例类
     */
    public static class Instance {

        /**
         * 是否启用集群模式
         */
        private boolean clusterEnabled;


        /**
         * zookeeper 地址集，在properties配置中以逗号分隔
         */
        private Set<String> zookeeperAddress = new LinkedHashSet<>();

        /**
         * canal服务端IP
         */
        private String host = "127.0.0.1";

        /**
         * canal服务端端口
         */
        private int port = 11111;

        /**
         * canal客户端用户名
         */
        private String userName = "";

        /**
         * canal客户端密码
         */
        private String password = "";

        /**
         * 每次从服务端获取事件消息的最大数量
         */
        private int batchSize = 1000;

        /**
         * 订阅过滤器
         */
        private String filter;

        /**
         * 发生异常后最多重试次数
         */
        private int retryCount = 10;

        /**
         * interval of the message-acquiring
         */
        private long acquireInterval = 2000;

        /**
         * which distributor classname to distribute the data from canal event
         */
        private String distributor;

        public Instance() {}

        public boolean isClusterEnabled() {
            return clusterEnabled;
        }

        public void setClusterEnabled(boolean clusterEnabled) {
            this.clusterEnabled = clusterEnabled;
        }

        public Set<String> getZookeeperAddress() {
            return zookeeperAddress;
        }

        public void setZookeeperAddress(Set<String> zookeeperAddress) {
            this.zookeeperAddress = zookeeperAddress;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public String getFilter() {
            return filter;
        }

        public void setFilter(String filter) {
            this.filter = filter;
        }

        public int getRetryCount() {
            return retryCount;
        }

        public void setRetryCount(int retryCount) {
            this.retryCount = retryCount;
        }

        public long getAcquireInterval() {
            return acquireInterval;
        }

        public void setAcquireInterval(long acquireInterval) {
            this.acquireInterval = acquireInterval;
        }

        public String getDistributor() {
            return distributor;
        }

        public void setDistributor(String distributor) {
            this.distributor = distributor;
        }
    }
}
