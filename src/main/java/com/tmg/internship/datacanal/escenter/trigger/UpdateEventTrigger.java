package com.tmg.internship.datacanal.escenter.trigger;

import com.tmg.internship.datacanal.escenter.exception.ESException;
import com.tmg.internship.datacanal.escenter.moduls.config.MappingNode;
import com.tmg.internship.datacanal.escenter.moduls.config.event.Condition;
import com.tmg.internship.datacanal.escenter.moduls.config.event.EventField;
import com.tmg.internship.datacanal.escenter.moduls.config.event.EventHandle;
import com.tmg.internship.datacanal.escenter.moduls.config.event.Trigger;
import com.tmg.internship.datacanal.escenter.moduls.index.MappingMap;
import com.tmg.internship.datacanal.escenter.parser.Event;
import com.tmg.utils.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 触发修改事件
 *
 * @author xiangjing
 * @date 2018/6/1
 * @company 天极云智
 */
public class UpdateEventTrigger extends AbstractESTrigger {

    @Override
    public void eventTrigger(MappingNode mappingSettings, MappingMap data) throws ESException, InterruptedException {
        if (data != null && mappingSettings != null) {
            //初始化 collectionNode
            parseCollectionNode(mappingSettings, data.getIndex());
            if (collectionNode != null) {
                //根据事件类型拿到触发事件
                Trigger trigger = collectionNode.getTrigger(Event.UPDATE);
                Map<String, Object> afterData = data.getData().getAfter();
                Map<String, Object> beforeData = data.getData().getBefore();
                if (trigger != null) {
                    List<EventField> eventFieldList = trigger.getFieldList();
                    int parseTime = 0;
                    for (EventField eventField : eventFieldList) {
                        if(parseTime>0){
                            Thread.sleep(500);
                        }
                        //where 为空
                        if(StringUtils.isTrimEmpty(eventField.getWhere())){
                            //代表更新的字段在级联更新设置里面
                            if (afterData != null && afterData.get(eventField.getColumnName()) != null) {
                                List<EventHandle> handleList = eventField.getHandleList();
                                for (EventHandle eventHandle : handleList) {
                                    parseEventHandle(eventHandle,afterData,beforeData,eventField);
                                }
                            }
                        }else {
                            String value = afterData.get(getWhereColum(eventField.getWhere())).toString();
                            String whereValue = getWhereValue(eventField.getWhere());
                            if (value.equals(whereValue)) {
                                if (afterData != null && afterData.get(eventField.getColumnName()) != null) {
                                    List<EventHandle> handleList = eventField.getHandleList();
                                    for (EventHandle eventHandle : handleList) {
                                        parseEventHandle(eventHandle,afterData,beforeData,eventField);
                                    }
                                }
                            }
                        }
                        parseTime++;
                    }
                }else {
                    return;
                }
            }
        } else {
            return;
        }
    }

    /**
     * 解析单个
     *
     * @param eventHandle
     * @param afterData
     * @param beforeData
     * @param eventField
     */
    private void parseEventHandle(EventHandle eventHandle,Map<String, Object> afterData,Map<String, Object> beforeData,EventField eventField){
        if(eventHandle !=null ){
            switch (eventHandle.getEvent()) {
                case DELETE:
                    //要删除文档数据的索引
                    String deleteIndex = checkIndexName(eventHandle.getFiled());
                    BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                    for (Condition condition : eventHandle.getConditionList()) {
                        boolQueryBuilder.must(QueryBuilders.termQuery(getIndexFileId(condition.getFieldName()), beforeData.get(condition.getColumnName())));
                    }
                    transportClientService.deleteByQuery(deleteIndex, boolQueryBuilder);
                    break;
                case INSERT:
                    //暂不考虑
                    break;
                case UPDATE:
                    //要更新文档数据的索引
                    String updateIndex = checkIndexName(getIndexName(eventHandle.getFiled()));
                    //要更新的字段
                    String fileId = getIndexFileId(eventHandle.getFiled());
                    //要更新的值
                    String value = StringUtils.isTrimEmpty(eventHandle.getValue()) ? afterData.get(eventField.getColumnName()).toString() : eventHandle.getValue();
                    Map<String, Object> params = new HashMap<>();
                    params.put(fileId, value);
                    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
                    for (Condition condition : eventHandle.getConditionList()) {
                        boolQuery.must(QueryBuilders.termQuery(checkConditionField(condition.getFieldName()), beforeData.get(condition.getColumnName())));
                    }
                    transportClientService.updateByQuery(updateIndex, 0, params, boolQuery);
                    break;
                default:
                    break;
            }
        }
    }


}
