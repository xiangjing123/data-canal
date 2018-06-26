package com.tmg.internship.datacanal.producer.listener;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.tmg.commons.mq.MessageProducer;
import com.tmg.commons.mq.model.MQMessageStatus;
import com.tmg.internship.datacanal.producer.common.OrderMessageProducer;
import com.tmg.utils.JsonUtils;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JsonConfig;
import net.sf.json.util.JSONStringer;
import net.sf.json.util.PropertyFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.sql.SQLType;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 习柚数据库DML事件监听器
 * 受到事件后，往MQ中发送即可
 *
 * @author Paul
 * @company 天极云智
 * @date 2018/4/14
 **/
@Component
public class XiyouDmlEventListener implements IDmlCanalEventListener {

    private final static Logger logger = LoggerFactory.getLogger(XiyouDmlEventListener.class);

    @Value("${canal.mqs.eventTopic}")
    private String MQS_CANAL_EVENT_TOPIC;
    @Value("${canal.mqs.insertEventKey}")
    private String MQS_CANAL_EVENT_INSERT_KEY;
    @Value("${canal.mqs.updateEventKey}")
    private String MQS_CANAL_EVENT_UPDATE_KEY;
    @Value("${canal.mqs.deleteEventKey}")
    private String MQS_CANAL_EVENT_DELETE_KEY;

    //在这里使用Resource是为了避免在canal server端已经堆积了消息，但是启动时，初始化MessageProducer比starter慢，造成的前面一些消息不能正常分发的问题
    //终极解决办法还是让applicationBean.xml优先加载，目前还没找到办法
//    @Resource
//    private MessageProducer messageProducer;

    @Override
    public void onInsert(String schemaName,String tableName,CanalEntry.RowData rowData) {

        String body = buildMQBodyForRowData(MQS_CANAL_EVENT_INSERT_KEY,rowData);

        //以  库名@表名  为tag，事件类型为Key，发送MQ消息
        MQMessageStatus status = OrderMessageProducer.orderSend(MQS_CANAL_EVENT_TOPIC, schemaName + "@" + tableName, MQS_CANAL_EVENT_INSERT_KEY, body);

        logger.debug("XiyouDML event {} send to mqs : {}",MQS_CANAL_EVENT_INSERT_KEY,status.equals(MQMessageStatus.SUCCESS));

    }

    @Override
    public void onUpdate(String schemaName,String tableName,CanalEntry.RowData rowData) {

        String body = buildMQBodyForRowData(MQS_CANAL_EVENT_UPDATE_KEY,rowData);

        //以  库名@表名  为tag，事件类型为Key，发送MQ消息
        MQMessageStatus status = OrderMessageProducer.orderSend(MQS_CANAL_EVENT_TOPIC, schemaName + "@" + tableName, MQS_CANAL_EVENT_UPDATE_KEY, body);

        logger.debug("XiyouDML event {} send to mqs : {}",MQS_CANAL_EVENT_UPDATE_KEY,status.equals(MQMessageStatus.SUCCESS));

    }

    @Override
    public void onDelete(String schemaName,String tableName,CanalEntry.RowData rowData) {

        String body = buildMQBodyForRowData(MQS_CANAL_EVENT_DELETE_KEY,rowData);

        //以  库名@表名  为tag，事件类型为Key，发送MQ消息
        MQMessageStatus status = OrderMessageProducer.orderSend(MQS_CANAL_EVENT_TOPIC, schemaName + "@" + tableName, MQS_CANAL_EVENT_DELETE_KEY, body);

        logger.debug("XiyouDML event {} send to mqs : {}",MQS_CANAL_EVENT_DELETE_KEY,status.equals(MQMessageStatus.SUCCESS));

    }

    /**
     * 根据事件类型构造mq消息体
     * @param mqsEventKey
     * @param rowData
     * @return
     */
    private String buildMQBodyForRowData(String mqsEventKey,CanalEntry.RowData rowData){

        StringBuilder sb = new StringBuilder();

        sb.append("{");
        sb.append("\"event\":\"").append(mqsEventKey).append("\",");
        sb.append("\"before\":");
        if(mqsEventKey.equals(MQS_CANAL_EVENT_UPDATE_KEY) || mqsEventKey.equals(MQS_CANAL_EVENT_DELETE_KEY)) {
            sb.append(columnsToJsonString(rowData.getBeforeColumnsList()));
        }else{
            sb.append("{}");
        }
        sb.append(",\"after\":");
        if(mqsEventKey.equals(MQS_CANAL_EVENT_INSERT_KEY)) {
            sb.append(columnsToJsonString(rowData.getAfterColumnsList()));
        }else if(mqsEventKey.equals(MQS_CANAL_EVENT_UPDATE_KEY)){
            sb.append(columnsToJsonString(rowData.getAfterColumnsList(),true));
        }else{
            sb.append("{}");
        }
        sb.append(",\"pk\":");
        sb.append(mqsEventKey.equals(MQS_CANAL_EVENT_INSERT_KEY)?primaryKeyToJsonArrayString(rowData.getAfterColumnsList()):primaryKeyToJsonArrayString(rowData.getBeforeColumnsList()));
        sb.append("}");

        return sb.toString();
    }

    private String columnsToJsonString(List<CanalEntry.Column> columns){
        return columnsToJsonString(columns,false);
    }

    /**
     * 构造数据json对象
     * @param columns
     * @param onlyUpdated 是否只构造有变更的字段
     * @return
     */
    private String columnsToJsonString(List<CanalEntry.Column> columns,boolean onlyUpdated){

        String json = "{}";

        Map<String,Object> columnMaps = new HashMap<String,Object>();

        for (CanalEntry.Column column : columns) {
            if(!onlyUpdated || (onlyUpdated && column.getUpdated())) {
                int type = column.getSqlType();
                Object v = column.getValue();

                try {
                    //尽量还原原始数据类型，特别是“数字”类型的，在生成json时不带引号，更利于消费端使用
                    //个别数据库里就为NULL的数字，就不强求了。。。
                    if (type == Types.BIGINT) {
                        v = Long.parseLong(column.getValue());
                    } else if (type == Types.INTEGER || type == Types.SMALLINT || type == Types.TINYINT) {
                        v = Integer.parseInt(column.getValue());
                    } else if (type == Types.FLOAT || type == Types.REAL) {
                        v = Float.parseFloat(column.getValue());
                    } else if (type == Types.DOUBLE) {
                        v = Double.parseDouble(column.getValue());
                    }
                }catch (NumberFormatException e){}

                columnMaps.put(column.getName(), v);
            }
        }

        try{
            JSONObject obj = JSONObject.fromObject(columnMaps);
            json = obj.toString();
        }catch(Exception e){
            logger.error("columnsToJsonString exception : " + e.getMessage());
        }

        return json;
    }

    /**
     * 构造主键字段json数组
     * @param columns
     * @return
     */
    private String primaryKeyToJsonArrayString(List<CanalEntry.Column> columns){

        StringBuilder sb = new StringBuilder();
        sb.append("[");

        for (CanalEntry.Column column : columns) {
            if(column.getIsKey()){
                sb.append("\"").append(column.getName()).append("\"").append(",");
            }
        }
        if(sb.length()>1){
            sb.setLength(sb.length()-1);
        }
        sb.append("]");
        return sb.toString();

    }
}
