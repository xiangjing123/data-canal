package com.tmg.internship.datacanal.producer.common;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.tmg.commons.mq.MessageProducer;
import com.tmg.commons.mq.model.DelayLevel;
import com.tmg.commons.mq.model.MQMessage;
import com.tmg.commons.mq.model.MQMessageStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 顺序消费生产者
 *
 * @author xiangjing
 * @date 2018/5/24
 * @company 天极云智
 */
public class OrderMessageProducer extends MessageProducer {
    private static Logger log = LoggerFactory.getLogger(OrderMessageProducer.class);
    private static DefaultMQProducer PRODUCER;
    private String namesrvAddr;

    public OrderMessageProducer() {
    }

    public OrderMessageProducer(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public void init() {
        if(PRODUCER == null) {
            log.info("初始化消息发送端........");
            PRODUCER = new DefaultMQProducer("Producer");
            PRODUCER.setNamesrvAddr(this.namesrvAddr);

            try {
                PRODUCER.start();
            } catch (MQClientException var2) {
                log.error("消息发送端初始化失败", var2);
            }
        }

    }

    public static MQMessageStatus orderSend(MQMessage msg, DelayLevel delay) {
        if(msg == null) {
            return MQMessageStatus.SUCCESS;
        } else if(PRODUCER == null) {
            log.info("MQ Producer has not configuration, message will not be send.");
            return MQMessageStatus.SUCCESS;
        } else {
            Message message = new Message();
            message.setTopic(msg.getTopic());
            message.setTags(msg.getSubTopic());
            message.setKeys(msg.getKey());
            message.setDelayTimeLevel(delay.ordinal());
            String data = msg.getData();
            message.setBody(((String)data).getBytes(Charset.forName("UTF-8")));
            Map userProperties = msg.getProperties();
            if(userProperties != null && !userProperties.isEmpty()) {
                Iterator e = userProperties.keySet().iterator();

                while(e.hasNext()) {
                    String uk = (String)e.next();
                    message.putUserProperty(uk, (String)userProperties.get(uk));
                }
            }

            try {
                SendResult e1 = PRODUCER.send(message, new MessageQueueSelector(){
                    @Override
                    public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                       int code= message.getTags().split("@")[0].hashCode();
                        log.debug("broker queue:"+code+""+message.getTags().split("@")[0]);
                        return list.get(code%list.size());
                    }
                },0);
                msg.setMsgId(e1.getMsgId());
                log.debug("Producer send a message :\n" + msg + "\n" + e1.toString());
                return MQMessageStatus.SUCCESS;
            } catch (MQClientException var7) {
                log.error(String.format("消息发送失败[%s]", new Object[]{message.toString()}), var7);
                return MQMessageStatus.FAIL;
            } catch (MQBrokerException var8) {
                log.error(String.format("消息发送失败[%s]", new Object[]{message.toString()}), var8);
                return MQMessageStatus.FAIL;
            } catch (RemotingException var9) {
                log.error(String.format("消息发送失败[%s]", new Object[]{message.toString()}), var9);
                return MQMessageStatus.FAIL;
            } catch (InterruptedException var10) {
                log.error(String.format("消息发送失败[%s]", new Object[]{message.toString()}), var10);
                return MQMessageStatus.FAIL;
            }
        }
    }

    public static MQMessageStatus orderSend(String topic, String subTopic, String key, String data) {
        MQMessage message = new MQMessage();
        message.setTopic(topic);
        message.setSubTopic(subTopic);
        message.setKey(key);
        message.setData(data);
        return orderSend(message);
    }

    public static MQMessageStatus orderSend(String topic, String subTopic, String key, String data, Map<String, String> userProperties) {
        MQMessage message = new MQMessage();
        message.setTopic(topic);
        message.setSubTopic(subTopic);
        message.setKey(key);
        message.setData(data);
        if(userProperties != null) {
            message.setProperties(userProperties);
        }

        return orderSend(message);
    }

    public static MQMessageStatus orderSend(MQMessage msg) {
        return orderSend(msg, DelayLevel.REAL_TIME);
    }

    @Override
    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    @Override
    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }
}
