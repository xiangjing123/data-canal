package com.tmg.internship.datacanal.consumer;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 消费者配置
 *
 * @author Paul
 * @company 天极云智
 * @date 2018/4/17
 **/
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
@ConfigurationProperties(prefix = "canal.mqs")
public class ConsumerConfig {

    /**
     * 是否启动canal事件消息队列的消费端
     */
    private boolean enabled;

    /**
     * MQ服务器地址
     */
    private String namesrvAddr;

    /**
     * 配置实例Map集合
     */
    private Map<String, Consumer> consumers = new LinkedHashMap<>();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public Map<String, Consumer> getConsumers() {
        return consumers;
    }

    public void setConsumers(Map<String, Consumer> consumers) {
        this.consumers = consumers;
    }

    /**
     * 实例类
     */
    public static class Consumer {

        /**
         * MQ服务器地址，如果没有配置，则沿用ConsumerConfig的主配置
         */
        private String namesrvAddr;

        /**
         * 订阅的主题名称
         */
        private String topic;

        /**
         * 库名+##+表名为一个tag，以双竖线分隔多个tag，或*表示订阅该topic下的所有tag
         * 如：  mysql##user||test##temp
         */
        private String tag;

        public Consumer(){}

        /**
         * IProcessor 的实现类，用于处理mq消息
         */
        private String processor;

        public String getNamesrvAddr() {
            return namesrvAddr;
        }

        public void setNamesrvAddr(String namesrvAddr) {
            this.namesrvAddr = namesrvAddr;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getTag() {
            return tag;
        }

        public void setTag(String tag) {
            this.tag = tag;
        }

        public String getProcessor() {
            return processor;
        }

        public void setProcessor(String processor) {
            this.processor = processor;
        }
    }
}
