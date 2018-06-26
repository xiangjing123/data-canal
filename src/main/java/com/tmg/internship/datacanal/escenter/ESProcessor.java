package com.tmg.internship.datacanal.escenter;

import com.tmg.commons.mq.model.MQMessage;
import com.tmg.internship.datacanal.consumer.IProcessor;
import com.tmg.internship.datacanal.escenter.executer.ExecuterStrategy;
import com.tmg.internship.datacanal.escenter.moduls.config.ESConfigure;
import com.tmg.internship.datacanal.escenter.parser.EventData;
import com.tmg.internship.datacanal.escenter.parser.ParserFactory;

/**
 * ES 数据中心入口
 * @author xiangjing
 * @date 2018/5/5
 * @company 天极云智
 */
public class ESProcessor implements IProcessor {

    private ESConfigure configure;

    public ESProcessor(ESConfigure configure) {
        this.configure = configure;
    }

    /**
     * MQ 数据
     */
    public static EventData data=null;

    @Override
    public void process(MQMessage message) throws Exception {


        data = ParserFactory.getParser(message.getKey()).parse(message);//解析MQ 文件
        ExecuterStrategy.getExecuter(configure).execute(data);//执行
    }
}
