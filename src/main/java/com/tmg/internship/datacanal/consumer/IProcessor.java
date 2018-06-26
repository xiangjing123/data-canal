package com.tmg.internship.datacanal.consumer;

import com.tmg.commons.mq.model.MQMessage;

/**
 * 消息处理接口
 *
 * @author Paul
 * @company 天极云智
 * @date 2018/4/17
 **/
public interface IProcessor {

    void process(MQMessage message) throws Exception;
}
