package com.tmg.internship.datacanal.escenter.moduls.config.event;

import com.tmg.internship.datacanal.escenter.common.BaseClass;
import com.tmg.internship.datacanal.escenter.parser.Event;

import java.util.List;

/**
 *
 *定义该数据集在不同事件时触发的数据处理规则，如果不需要触发对应的事件，则留空或不配置
 * @author xiangjing
 * @date 2018/5/11
 * @company 天极云智
 */
public class Trigger  extends BaseClass {

    /**
     * 事件触发的类型：数据源发生 的Event事件时触发以下操作
     */
    private Event event;

    /**
     * 触发属性如果是insert 和delete 可以不设置columnName
     */
    private List<EventField> fieldList;

    public Event getEvent() {
        return event;
    }

    public void setEvent(Event event) {
        this.event = event;
    }

    public List<EventField> getFieldList() {
        return fieldList;
    }

    public void setFieldList(List<EventField> fieldList) {
        this.fieldList = fieldList;
    }

    @Override
    public String toString() {
        return "{" +
                "event=" + event +
                ", fieldList=" + fieldList +
                '}';
    }
}
