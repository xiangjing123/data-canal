package com.tmg.internship.datacanal.producer.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.tmg.internship.datacanal.producer.CanalConfig;
import com.tmg.internship.datacanal.producer.distributor.AbstractDistributor;
import com.tmg.internship.datacanal.producer.distributor.DmlDistributor;
import com.tmg.internship.datacanal.producer.listener.ICanalEventListener;
import com.tmg.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *  CanalClient的抽象父类，完成canal connector的初始化等工作
 *
 * @author Paul
 * @company 天极云智
 * @date 2018/4/13
 **/
public class SimpleCanalClient implements ICanalClient {

//    private final static Logger logger = LoggerFactory.getLogger(SimpleCanalClient.class);

    /**
     * running flag
     */
    private boolean running;

    /**
     * 自定义canal配置，在application.properties中以canal.client.instances.{instance_name}为各实例的配置项
     */
    private CanalConfig canalConfig;

    //用线程池来管理
    private ThreadPoolExecutor executor;

    private List<ICanalEventListener> listeners = new ArrayList<>();

    public SimpleCanalClient(CanalConfig canalConfig) {
        Objects.requireNonNull(canalConfig, "canalConfig can not be null!");
        this.canalConfig = canalConfig;

        executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<>(), Executors.defaultThreadFactory());

    }

    @Override
    public void start() {
        Map<String, CanalConfig.Instance> instanceMap = getConfig();
        for (Map.Entry<String, CanalConfig.Instance> instanceEntry : instanceMap.entrySet()) {
            process(processInstanceEntry(instanceEntry), instanceEntry);
        }
    }

    /**
     * To initialize the canal connector
     * @param connector CanalConnector
     * @param config config
     */
    protected void process(CanalConnector connector, Map.Entry<String, CanalConfig.Instance> config) {
        String distributor = config.getValue().getDistributor();

        try {
            Class<?> cls = Class.forName(distributor);
            Constructor<?> cons = cls.getConstructor(CanalConnector.class, Map.Entry.class,List.class);
            Object obj = cons.newInstance(connector, config, listeners);

            if(obj instanceof Runnable) {
                executor.submit((Runnable) obj);
            }
        }catch (ClassNotFoundException e){
            throw new CanalClientException("distributor class not found : " + distributor);
        }catch (NoSuchMethodException e){
            throw new CanalClientException("distributor class doesn't have constructor as AbstractDistributor!");
        }catch (Exception e){
            throw new CanalClientException("distributor instantiating failed : " + distributor);
        }
    }

    private CanalConnector processInstanceEntry(Map.Entry<String, CanalConfig.Instance> instanceEntry) {
        CanalConfig.Instance instance = instanceEntry.getValue();
        CanalConnector connector;
        if (instance.isClusterEnabled()) {
            List<SocketAddress> addresses = new ArrayList<>();
            for (String s : instance.getZookeeperAddress()) {
                String[] entry = s.split(":");
                if (entry.length != 2)
                    throw new CanalClientException("error parsing zookeeper address:" + s);
                addresses.add(new InetSocketAddress(entry[0], Integer.parseInt(entry[1])));
            }
            connector = CanalConnectors.newClusterConnector(addresses, instanceEntry.getKey(),
                    instance.getUserName(),
                    instance.getPassword());
        } else {
            connector = CanalConnectors.newSingleConnector(new InetSocketAddress(instance.getHost(), instance.getPort()),
                    instanceEntry.getKey(),
                    instance.getUserName(),
                    instance.getPassword());
        }
        connector.connect();
        if (!StringUtils.isEmpty(instance.getFilter())) {
            connector.subscribe(instance.getFilter());
        } else {
            // 经过阅读官方文档和实验，客户端的subscribe正则会覆盖掉服务端的该配置
            // 也就是说一旦客户端配置成关注所有库表，就会造成服务端的配置失效，会将所有库表的事件都传过来，除非服务端设置了黑名单的
            // 所以这里默认不加所有库表的subscribe了
//            connector.subscribe(".*\\..*");
        }

        connector.rollback();
        return connector;
    }

    /**
     * get the config
     *
     * @return config
     */
    protected Map<String, CanalConfig.Instance> getConfig() {
        CanalConfig config = canalConfig;
        Map<String, CanalConfig.Instance> instanceMap;
        if (config != null && (instanceMap = config.getInstances()) != null && !instanceMap.isEmpty()) {
            return config.getInstances();
        } else {
            throw new CanalClientException("can not get the configuration of canal client!");
        }
    }

    @Override
    public void stop() {
        setRunning(false);
        executor.shutdown();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void setRunning(boolean running) {
        this.running = running;
    }

    @Override
    public void addListener(ICanalEventListener listener){
        this.listeners.add(listener);
    }
}
