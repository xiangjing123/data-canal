package com.tmg.internship.datacanal.escenter.parser;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.tmg.commons.mq.model.MQMessage;
import com.tmg.internship.datacanal.escenter.common.EnumUtil;
import com.tmg.internship.datacanal.escenter.exception.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 普通的数据解析器，只解析数据
 *
 * @author xiangjing
 * @date 2018/5/7
 * @company 天极云智
 */

public class BaseEventParser implements  EventParser{

    public static final Logger logger = LoggerFactory.getLogger(BaseEventParser.class);

    public static EventData data= null;

    /**
     * 解析基本数据格式，
     * @param message
     * @return
     * @throws ParseException
     */
    @Override
    public EventData parse(MQMessage message) throws ParseException {

         data= new EventData(DataType.BASE);//基本数据格式

        JSONObject jsonObject = JSON.parseObject(message.getData());
        Event event = EnumUtil.parseEvent(message.getKey());
        if( null != event){
            data.setEvent(event);
        }else{
            throw new ParseException(message.getMsgId()+"{},"+" data:"+message.getData()+" 数据中 event 未识别");
        }
        //获取数据库@表名 结构
        data.setIndex(message.getSubTopic());
        data.setBefore(toMap(jsonObject, JSONKeys.BEFORE));
        data.setAfter(toMap(jsonObject, JSONKeys.AFTER));
        data.setKeys(toArray(jsonObject, JSONKeys.PK));
        return data;
    }


    /**
     * 转换为map
     * @param json
     * @param key
     * @return
     */
    public Map<String,Object> toMap(JSONObject json,JSONKeys key){
        Object obj = json.get(key.getCode());
        if(obj == null){
            return Collections.emptyMap();
        }else{
            return JSON.parseObject(obj.toString(),Map.class);
        }
    }



    /**
     * 获取json 数据并处理空值问题
     * @param data
     * @param key
     * @return
     */
    public String toString(JSONObject data,JSONKeys key){
        Object obj = data.get(key.getCode());
        return obj == null ? null : obj.toString();
    }

    /**
     * 将数据转换为List
     * @param data
     * @param key
     * @return
     */
    public List<String> toArray(JSONObject data,JSONKeys key){
        Object obj = data.get(key.getCode());
        return obj == null ? Collections.emptyList() : JSON.parseArray(obj.toString(), String.class);
    }

}
