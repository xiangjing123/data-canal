package com.tmg.internship.datacanal.producer;

import com.tmg.commons.mq.MessageProducer;
import com.tmg.internship.datacanal.producer.client.ICanalClient;
import com.tmg.internship.datacanal.producer.client.SimpleCanalClient;
import com.tmg.internship.datacanal.producer.common.OrderMessageProducer;
import com.tmg.internship.datacanal.producer.listener.XiyouDmlEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 在springboot启动时初始化并启动canal客户端
 *
 * @author Paul
 * @company 天极云智
 * @date 2018/4/13
 **/
@Component
public class CanalClientStarter {

    private final static Logger logger = LoggerFactory.getLogger(CanalClientStarter.class);

    @Autowired
    private CanalConfig canalConfig;

    @Resource
    private XiyouDmlEventListener xiyouDmlEventListener;


    @Value("${mqs.namesrvAddr}")
    private String mqsNamesrvAddr;


//    @Bean
//    private ICanalClient canalClient() {
//        if(canalConfig.isEnabled()) {
//            logger.info("Starting canal client....");
//            ICanalClient canalClient = new SimpleCanalClient(canalConfig);
//            canalClient.addListener(xiyouDmlEventListener);
//            canalClient.start();
//            return canalClient;
//        }else{
//            logger.info("Canal client is not enabled , starting cancelled");
//            return null;
//        }
//    }


    @EventListener
    public void onApplicationEvent(ContextRefreshedEvent event) {
        if(event.getApplicationContext().getParent() == null) {
            logger.debug("onApplicationEvent*************************************");

            if (canalConfig.isEnabled()) {

                ApplicationContext context = event.getApplicationContext();
                if (!context.containsBean("messageProducer")) {

                    logger.debug("Init MessageProducer....");

                    class MyMessageProducer extends OrderMessageProducer {
                        public void doInit() {
                            this.init();
                        }
                    }
                    ;

                    MyMessageProducer messageProducer = new MyMessageProducer();
                    messageProducer.setNamesrvAddr(mqsNamesrvAddr);
                    messageProducer.doInit();

                }

                logger.info("Starting canal client....");
                ICanalClient canalClient = new SimpleCanalClient(canalConfig);
                canalClient.addListener(xiyouDmlEventListener);
                canalClient.start();
            } else {
                logger.info("Canal client is not enabled , starting cancelled");
            }
        }
    }
}
