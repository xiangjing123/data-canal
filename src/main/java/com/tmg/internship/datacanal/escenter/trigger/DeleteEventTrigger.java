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
 * 触发删除事件
 *
 * @author xiangjing
 * @date 2018/6/1
 * @company 天极云智
 */
public class DeleteEventTrigger extends AbstractESTrigger {
    @Override
    public void eventTrigger(MappingNode mappingSettings, MappingMap data) throws ESException, InterruptedException {
        if (data != null && mappingSettings != null) {
            //初始化 collectionNode
            parseCollectionNode(mappingSettings, data.getIndex());
            //得到删除之前的数据
            Map<String, Object> beforeData = data.getData().getBefore();
            if (collectionNode != null && beforeData != null) {
                //根据事件类型拿到触发事件
                Trigger trigger = collectionNode.getTrigger(Event.DELETE);
                if (trigger != null) {
                    List<EventField> eventFieldList = trigger.getFieldList();
                    int parseTime = 0;
                    for (EventField eventField : eventFieldList) {
                        if (parseTime > 0) {
                            Thread.sleep(500);
                        }
                        List<EventHandle> handleList = eventField.getHandleList();
                        //如果where不为空
                        if (!StringUtils.isTrimEmpty(eventField.getWhere())) {
                            String value = beforeData.get(getWhereColum(eventField.getWhere())).toString();
                            String whereValue = getWhereValue(eventField.getWhere());
                            if (value.equals(whereValue)) {
                                for (EventHandle eventHandle : handleList) {
                                    parseEventHandle(eventHandle, beforeData);
                                }
                            } else {
                                return;
                            }
                        } else {
                            for (EventHandle eventHandle : handleList) {
                                parseEventHandle(eventHandle, beforeData);
                            }
                        }
                        parseTime++;
                    }
                } else {
                    return;
                }
            }
        }
    }

    /**
     * 处理单个
     *
     * @param eventHandle
     * @param beforeData
     */
    private void parseEventHandle(EventHandle eventHandle, Map<String, Object> beforeData) {
        if (eventHandle != null && beforeData != null) {
            switch (eventHandle.getEvent()) {
                case UPDATE:
                    //要更新文档数据的索引
                    String updateIndex = checkIndexName(getIndexName(eventHandle.getFiled()));
                    //要更新的字段
                    String fileId = getIndexFileId(eventHandle.getFiled());
                    //要更新的值
                    String updateValue = eventHandle.getValue();
                    if (StringUtils.isTrimEmpty(updateValue)) {
                        break;
                    }
                    Map<String, Object> params = new HashMap<>();
                    params.put(fileId, updateValue);
                    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
                    for (Condition condition : eventHandle.getConditionList()) {
                        boolQuery.must(QueryBuilders.termQuery(checkConditionField(condition.getFieldName()), beforeData.get(condition.getColumnName())));
                    }
                    transportClientService.updateByQuery(updateIndex, 0, params, boolQuery);
                    break;
                case INSERT:
                    //暂不考虑
                    break;
                case DELETE:
                    String deleteIndex = checkIndexName(eventHandle.getFiled());
                    BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                    for (Condition condition : eventHandle.getConditionList()) {
                        boolQueryBuilder.must(QueryBuilders.termQuery(getIndexFileId(condition.getFieldName()), beforeData.get(condition.getColumnName())));
                    }
                    transportClientService.deleteByQuery(deleteIndex, boolQueryBuilder);
                    break;
                default:
                    break;
            }
        }
    }
}
