package com.tmg.internship.datacanal.datainit;
import com.tmg.commons.mq.MessageProducer;
import com.tmg.internship.datacanal.producer.common.OrderMessageProducer;
import com.tmg.utils.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

/**
 * 数据库配置
 *
 * @author xiangjing
 * @date 2018/5/23
 * @company 天极云智
 */
@Component
@PropertySource(value="classpath:dataInit.properties")
public class DatabaseConfig {

    @Value("${jdbc.driver}")
    private String driverName;

    @Value("${jdbc.url}")
    private String url;

    @Value("${jdbc.userName}")
    private String username;

    @Value("${jdbc.passWord}")
    private String passwod;

    @Value("${mqs.namesrvAddr}")
    private String mqsNamesrvAddr;

    @Value("${init.defaultOpen}")
    private Boolean defaultOpen;

    @Value("${init.tables}")
    private String initTables;

    @Value("${init.unclude.tables}")
    private String uncludeTables;

    @Value("${canal.mqs.eventTopic}")
    private String MQS_CANAL_EVENT_TOPIC;
    @Value("${canal.mqs.insertEventKey}")
    private String MQS_CANAL_EVENT_INSERT_KEY;


    @Bean(initMethod = "init",destroyMethod = "close")
    public OrderMessageProducer getMessageProducer(){
        OrderMessageProducer messageProducer = new OrderMessageProducer(mqsNamesrvAddr);
        return messageProducer;
    }

    @Bean(initMethod = "initData")
    public DataBaseInit initDatabase(MessageProducer messageProducer)  {
        DataBaseInit dataBaseInit = new DataBaseInit();
        dataBaseInit.setMQS_CANAL_EVENT_TOPIC(this.MQS_CANAL_EVENT_TOPIC);
        String initTables = System.getProperty("initTables");

        if(StringUtils.isEmpty(initTables)){
            dataBaseInit.setInitTables(this.initTables);
            dataBaseInit.setInit_unclude_tables(this.uncludeTables);
        }else{
            dataBaseInit.setInitTables(initTables);
            dataBaseInit.setInit_unclude_tables(null);
        }
        dataBaseInit.setDefaultOpen(this.defaultOpen);
        dataBaseInit.setDriverName(this.driverName);
        dataBaseInit.setConnectionUrl(this.url);
        dataBaseInit.setUsername(this.username);
        dataBaseInit.setPasswod(this.passwod);
        return dataBaseInit;

    }

}

