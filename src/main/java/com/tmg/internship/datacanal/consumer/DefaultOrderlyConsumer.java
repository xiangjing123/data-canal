package com.tmg.internship.datacanal.consumer;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.*;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.tmg.commons.mq.model.MQMessage;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * 默认消费者，不实现具体的消息处理，只提供基本的处理流程
 *
 * @author Paul
 * @company 天极云智
 * @date 2018/4/17
 **/
public class DefaultOrderlyConsumer implements IConsumer{

    private final static Logger logger = LoggerFactory.getLogger(DefaultOrderlyConsumer.class);

    /**
     * running flag
     */
    private boolean running;

    /**
     * ES 数据处理器
     */
    private IProcessor processor;

    private ConsumerConfig consumerConfig;

    public DefaultOrderlyConsumer(ConsumerConfig consumerConfig){
        Objects.requireNonNull(consumerConfig, "consumerConfig can not be null!");
        this.consumerConfig = consumerConfig;
    }

    /**
     * 从配置中初始化所有消费者，并启动
     */
    @Override
    public void start() throws MQClientException {
        Map<String, ConsumerConfig.Consumer> consumerMap = getConfig();
        for (Map.Entry<String, ConsumerConfig.Consumer> entry : consumerMap.entrySet()) {
            startConsumer(entry);
        }
    }

    /**
     * 启动单个消费者实例
     * @param entry
     */
    protected void startConsumer(Map.Entry<String, ConsumerConfig.Consumer> entry) throws MQClientException {

        ConsumerConfig.Consumer config = entry.getValue();

        String group = this.getClass().getName().replaceAll("\\.","-");
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);

        logger.debug("startConsumer : " + group);

        //如果单个consumer没有配置MQ服务器地址，则使用全局的MQ服务器配置
        consumer.setNamesrvAddr(StringUtils.isEmpty(config.getNamesrvAddr())?this.consumerConfig.getNamesrvAddr():config.getNamesrvAddr());
        consumer.subscribe(config.getTopic(), config.getTag());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.registerMessageListener(
                new MessageListenerOrderly() {
                    @Override
                    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext Context) {


                        MessageExt msg = list.get(0);
                        MQMessage message = new MQMessage();
                        message.setTopic(msg.getTopic());
                        message.setSubTopic(msg.getTags());
                        message.setKey(msg.getKeys());
                        message.setReconsumeTimes(msg.getReconsumeTimes());
                        message.setProperties(msg.getProperties());

                        String bodyStr = null;
                        try {
                            bodyStr = new String(msg.getBody(), "UTF-8");
                        } catch (UnsupportedEncodingException e) {
                            logger.warn("UnsupportedEncodingException" + e.getMessage());
                            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                        }

                        message.setData(bodyStr);
                        try {
                            processor.process(message);
                            logger.info("RocketMQConsumer message processed success, message is: msgId=" + msg.getMsgId()+", tags="+msg.getTags());
                            return ConsumeOrderlyStatus.SUCCESS;
                        } catch (Exception e) {
                            logger.error("RocketMQConsumer message processed failed, try it later! message is: msgId=" +msg.getMsgId()+", tags="+msg.getTags(),e);
                            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                        }

                    }
                }
        );
        consumer.start();

    }

    @Override
    public void stop() {
        setRunning(false);
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void setRunning(boolean running) {
        this.running = running;
    }

    public IProcessor getProcessor() {
        return processor;
    }

    public void setProcessor(IProcessor processor) {
        this.processor = processor;
    }

    protected Map<String, ConsumerConfig.Consumer> getConfig() {
        ConsumerConfig config = consumerConfig;
        Map<String, ConsumerConfig.Consumer> consumerMap;
        if (config != null && (consumerMap = config.getConsumers()) != null && !consumerMap.isEmpty()) {
            return config.getConsumers();
        } else {
            throw new NoSuchElementException("can not get the configuration of canal.mqs.consumers!");
        }
    }
}
