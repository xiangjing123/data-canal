package com.tmg.internship.datacanal.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

/**
 * 在springboot启动时初始化并启动消费者
 *
 * @author Paul
 * @company 天极云智
 * @date 2018/4/17
 **/
@Component
public class ConsumerStarter {

    private final static Logger logger = LoggerFactory.getLogger(ConsumerStarter.class);

    @Autowired
    private ConsumerConfig consumerConfig;

    @Bean
    public IConsumer consumer(IProcessor processor) throws Exception {
        if(consumerConfig.isEnabled()) {
            logger.info("Starting canal consumer....");

            //对应binlog要尽量使用“顺序”消费，这样才能保证“还原”原始数据状态
            DefaultOrderlyConsumer consumer = new DefaultOrderlyConsumer(consumerConfig);
            consumer.setProcessor(processor);
            consumer.start();

            return consumer;
        }else{
            logger.info("Canal consumer is not enabled , starting cancelled");
            return null;
        }
    }
}
