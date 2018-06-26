package com.tmg.internship.datacanal.escenter.esengine;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;


/**
 * @author chensl [cookchensl@gmail.com]
 * @date 2018/5/10 11:35
 * @description es配置  客户端
 */

@Configuration
public class ClientConfig {
    private final static Logger logger = LoggerFactory.getLogger(ClientConfig.class);
    /**
     * es服务器地址，多个用逗号隔开
     */
    @Value("${es.highLevelClient.serviceAddress}")
    private String serviceAddress;

    /**
     * 客户端和服务器建立连接的超时设置
     */
    @Value("${es.highLevelClient.connectTimeOut}")
    private int connectTimeOut;

    /**
     * 客户端从服务器读取数据的超时设置
     */
    @Value("${es.highLevelClient.socketTimeOut}")
    private int socketTimeOut;

    /**
     * 从连接池获取连接的超时设置
     */
    @Value("${es.highLevelClient.connectionRequestTimeOut}")
    private int connectionRequestTimeOut;

    /**
     * 最大连接数
     */
    @Value("${es.highLevelClient.maxConnectNum}")
    private int maxConnectNum;

    /**
     * 单个主机最大连接数
     */
    @Value("${es.highLevelClient.maxConnectPerRoute}")
    private int maxConnectPerRoute;


    /**
     * 集群名称
     */
    @Value("${es.cluster.name}")
    private String clusterName;

    /**
     * es服务器地址，多个用逗号隔开
     */
    @Value("${es.transport.serviceAddress}")
    private String serviceAddressForTransport;

    /**
     * 自动嗅探配置
     */
    @Value("${es.transport.sniff}")
    private boolean sniff;

    /**
     * 设置 true ，忽略连接节点集群名验证
     */
    @Value("${es.transport.sniff}")
    private boolean ignoreClusterName;

    /**
     * ping一个节点的响应时间 默认5秒
     */
    @Value("${es.transport.ping_timeout}")
    private String pingTimeOut;

    /**
     * sample/ping 节点的时间间隔，默认是5s
     */
    @Value("${es.transport.nodes_sampler_interval}")
    private String nodesSamplerInterval;


    private RestClientBuilder builder;

    @Bean
    public RestHighLevelClient init() throws Exception {
        RestHighLevelClient restHighLevelClient = null;
        Set<HttpHost> httpHostSet = new HashSet<>();
        for (String s : serviceAddress.split(",")) {
            String[] entry = s.split(":");
            if (entry.length != 2) {
                throw new ElasticsearchException("error parsing es service address:" + s);
            } else {
                httpHostSet.add(new HttpHost(entry[0], Integer.parseInt(entry[1]), "http"));
            }
        }
        builder = RestClient.builder(httpHostSet.toArray(new HttpHost[httpHostSet.size()]));
        setConnectTimeOutConfig();
        setMutiConnectConfig();
        restHighLevelClient = new RestHighLevelClient(builder);

        return restHighLevelClient;
    }

    /**
     * 配置连接时间延时
     */
    private void setConnectTimeOutConfig() {
        builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            @Override
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                requestConfigBuilder.setConnectTimeout(connectTimeOut);
                requestConfigBuilder.setSocketTimeout(socketTimeOut);
                requestConfigBuilder.setConnectionRequestTimeout(connectionRequestTimeOut);
                return requestConfigBuilder;
            }
        });
    }

    /**
     * 使用异步httpclient时设置并发连接数
     */
    private void setMutiConnectConfig() {
        builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                httpClientBuilder.setMaxConnTotal(maxConnectNum);
                httpClientBuilder.setMaxConnPerRoute(maxConnectPerRoute);
                return httpClientBuilder;
            }
        });
    }


    @Bean
    public TransportClient initTransport() throws Exception {
        Settings settings = Settings.builder()
                .put("cluster.name", clusterName)
                .put("client.transport.sniff", sniff)
                .put("client.transport.ignore_cluster_name", ignoreClusterName)
                .put("client.transport.ping_timeout", pingTimeOut)
                .put("client.transport.nodes_sampler_interval", nodesSamplerInterval)
                .build();
        TransportClient client = new PreBuiltTransportClient(settings);
        for (String s : serviceAddressForTransport.split(",")) {
            String[] entry = s.split(":");
            if (entry.length != 2) {
                throw new ElasticsearchException("error parsing es service address:" + s);
            } else {
                client.addTransportAddress(new TransportAddress(InetAddress.getByName(entry[0]), Integer.parseInt(entry[1])));
            }
        }
        return client;
    }

}
