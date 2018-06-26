package com.tmg.internship.datacanal.escenter.moduls.config.event;

import com.tmg.internship.datacanal.escenter.common.BaseClass;

import java.util.List;

/**
 * 事件触发的filed 配置
 *
 * @author xiangjing
 * @date 2018/5/11
 * @company 天极云智
 */
public class EventField  extends BaseClass {

    //源数据列属性
    private String columnName;

    //列触发条件
    private String where;

    //需要进行级联更新的操作
    private List<EventHandle> handleList;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getWhere() {
        return where;
    }

    public void setWhere(String where) {
        this.where = where;
    }

    public List<EventHandle> getHandleList() {
        return handleList;
    }

    public void setHandleList(List<EventHandle> handleList) {
        this.handleList = handleList;
    }

    @Override
    public String toString() {
        return "{" +
                "columnName='" + columnName + '\'' +
                ", where='" + where + '\'' +
                ", handleList=" + handleList +
                '}';
    }
}
