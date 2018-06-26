package com.tmg.internship.datacanal.escenter.esconfig;

import com.tmg.commons.utils.SpringUtils;
import com.tmg.internship.datacanal.consumer.IProcessor;
import com.tmg.internship.datacanal.escenter.ESProcessor;
import com.tmg.internship.datacanal.escenter.moduls.config.ConfigParser;
import com.tmg.internship.datacanal.escenter.moduls.config.DefaultConfigureParser;
import com.tmg.internship.datacanal.escenter.moduls.config.ESConfigure;
import com.tmg.internship.datacanal.escenter.moduls.notice.AsynSendSMS;
import com.tmg.utils.SendSMS;
import com.tmg.utils.sms.handler.MontnetsResponseHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * ES springboot 的配置
 *
 * @author xiangjing
 * @date 2018/5/16
 * @company 天极云智
 */
@Configuration
@PropertySource(value = "classpath:sms.properties",encoding ="UTF-8")
public class ESConfig {

    @Value("${sms.url}")
    private String url;

    @Value("${sms.userNameParameter}")
    private String userNameParameter;

    @Value("${sms.passwordParameter}")
    private String passwordParameter;

    @Value("${sms.phoneParameter}")
    private String phoneParameter;

    @Value("${sms.messageParameter}")
    private String messageParameter;

    @Value("${sms.userName}")
    private String userName;

    @Value("${sms.password}")
    private String password;

    @Value("${send.msg.maxSendNum}")
    private int maxSendNum;

    @Value("${send.msg.phone}")
    private String phone;

    @Value("${send.msg.admin}")
    private String admin;



    /**
     * es config
     * @return
     * @throws Exception
     */
    @Bean(name = "esConfigure")
    public ESConfigure initESconfig() throws Exception {
        ConfigParser parser = new DefaultConfigureParser();
        ESConfigure configure = parser.parseConfig();
        return configure;
    }

    /**
     * ES 处理器
     * @param esConfigure
     * @return
     */
    @Bean("processor")
    public IProcessor getProcessor(ESConfigure esConfigure){
        ESProcessor processor = new ESProcessor(esConfigure);
        return processor;
    }

    @Bean
    public MontnetsResponseHandler getHandle(){
        MontnetsResponseHandler handler =new MontnetsResponseHandler();
        return handler;
    }

    @Bean
    public SpringUtils getSpringUtils(){
        SpringUtils springUtils = new SpringUtils();
        return springUtils;
    }

    @Bean(initMethod = "init")
    public SendSMS getSendSMS(MontnetsResponseHandler handler){
        SendSMS sendSMS = new SendSMS();
        sendSMS.setUrl(url);
        sendSMS.setMessageParameter(messageParameter);
        sendSMS.setPassword(password);
        sendSMS.setPasswordParameter(passwordParameter);
        sendSMS.setPhoneParameter(phoneParameter);
        sendSMS.setUserName(userName);
        sendSMS.setUserNameParameter(userNameParameter);
        sendSMS.setHandler(handler);
        return sendSMS;
    }

    /**
     * ES 数据中心 短信发送器
     * @return
     */
    @Bean(name = "asynSendSMS",destroyMethod = "destory")
    public AsynSendSMS getAsynSendSMS(){
        AsynSendSMS asynSendSMS = new AsynSendSMS(maxSendNum,phone,admin);
        return asynSendSMS;
    }
}
