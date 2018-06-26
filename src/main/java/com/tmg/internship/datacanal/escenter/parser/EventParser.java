package com.tmg.internship.datacanal.escenter.parser;

import com.tmg.commons.mq.model.MQMessage;
import com.tmg.internship.datacanal.escenter.exception.ParseException;

/**
 * 事件解析器
 *
 * @author xiangjing
 * @date 2018/5/5
 * @company 天极云智
 */
public interface EventParser {

    /**
     *  解析MQ 消息队列传过来的数据
     * @param message
     * @return
     * @throws ParseException
     */
    EventData parse(MQMessage message) throws ParseException;


}
