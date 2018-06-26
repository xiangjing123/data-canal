package com.tmg.internship.datacanal.producer.distributor;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.tmg.internship.datacanal.producer.CanalConfig;
import com.tmg.internship.datacanal.producer.listener.ICanalEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * canal事件分发基础类
 *
 * @author Paul
 * @company 天极云智
 * @date 2018/4/13
 **/
public abstract class AbstractDistributor implements Runnable{

    
    /**
     * canal connector
     */
    private final CanalConnector connector;

    /**
     * custom config
     */
    protected final CanalConfig.Instance config;

    /**
     * destination of canal server
     */
    protected final String destination;

    /**
     * listeners which are used by implementing the Interface
     */
    protected final List<ICanalEventListener> listeners = new ArrayList<>();

    /**
     * running flag
     */
    private volatile boolean running = true;

    private static final Logger logger = LoggerFactory.getLogger(AbstractDistributor.class);

    public AbstractDistributor(CanalConnector connector,
                                      Map.Entry<String, CanalConfig.Instance> config,
                                      List<ICanalEventListener> listeners) {
        Objects.requireNonNull(connector, "connector can not be null!");
        Objects.requireNonNull(config, "config can not be null!");
        this.connector = connector;
        this.destination = config.getKey();
        this.config = config.getValue();
        if (listeners != null)
            this.listeners.addAll(listeners);
    }

    @Override
    public void run() {
        int errorCount = config.getRetryCount();
        final long interval = config.getAcquireInterval();
        final String threadName = Thread.currentThread().getName();
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                Message message = connector.getWithoutAck(config.getBatchSize());
                long batchId = message.getId();
                int size = message.getEntries().size();
                logger.debug("{}: Get message from canal server >>>>> size:{}", threadName, size);
                //empty message
                if (batchId == -1 || size == 0) {
                    logger.debug("{}: Empty message... sleep for {} millis", threadName, interval);
                    Thread.sleep(interval);
                } else {
                    distributeEvent(message);
                }
                // commit ack
                connector.ack(batchId);
                logger.debug("{}: Ack message. batchId:{}", threadName, batchId);

                //正常连接，恢复异常计数
                errorCount = config.getRetryCount();

            } catch (CanalClientException e) {
                errorCount--;
                logger.error(threadName + ": Error occurred : {}", e.getMessage());
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e1) {
                    errorCount = 0;
                }
                //如果是IO错误，有可能是网络连接问题，尝试重连
                if(e.getCause() instanceof IOException) {
                    try {
                        logger.info(threadName + " retry connecting ...");
                        connector.connect();
                    }catch (CanalClientException ec) {
                        logger.info(threadName + " reconnect failed : " , ec);
                    }
                }
            } catch (InterruptedException e) {
                errorCount = 0;
                connector.rollback();
            } finally {
                if (errorCount <= 0) {
                    stop();
                    logger.info("{}: Topping the client.. ", Thread.currentThread().getName());
                }
            }
        }
        stop();
        logger.info("{}: client stopped. ", Thread.currentThread().getName());
    }

    /**
     * to distribute the message to special event and let the event listeners to handle it
     *
     * @param message canal message
     */
    protected abstract void distributeEvent(Message message);

    /**
     * stop running
     */
    void stop() {
        running = false;
    }

}
