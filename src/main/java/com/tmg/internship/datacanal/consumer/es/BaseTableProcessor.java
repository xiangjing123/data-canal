package com.tmg.internship.datacanal.consumer.es;

import com.tmg.commons.mq.model.MQMessage;
import com.tmg.internship.datacanal.consumer.IProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 习柚基础表数据处理入es索引
 *
 * @author Paul
 * @company 天极云智
 * @date 2018/4/17
 **/
public class BaseTableProcessor implements IProcessor {

    private final static Logger logger = LoggerFactory.getLogger(BaseTableProcessor.class);

    @Override
    public void process(MQMessage message) throws Exception {

        logger.debug("BaseTableProcessor process : {}",message.toString());

    }

//    @Override
//    public void run() {
//
//    }
}
