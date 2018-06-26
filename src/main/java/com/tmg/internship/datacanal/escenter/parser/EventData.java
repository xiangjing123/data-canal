package com.tmg.internship.datacanal.escenter.parser;

import java.util.List;
import java.util.Map;

/**
 * 解析数据结果
 *
 * @author xiangjing
 * @date 2018/5/7
 * @company 天极云智
 */
public class EventData {

    //数据库@表
    private String index;

    //数据类型
    private DataType dataType;

    //事件类型
    private Event event;

    //数据更新之前
    private Map<String,Object> before;

    //数据更新之后
    private Map<String,Object> after;

    //主键
    private List<String> keys;

    public EventData() {

    }

    public EventData(DataType dataType) {
        this.dataType = dataType;
    }

    /**
     * 获取数据库@表 的索引结构
     * @return
     */
    public String getIndex() {
        return index;
    }

    /**
     * 设置数据库@表 的索引结构
     * @return
     */
    public void setIndex(String index) {
        this.index = index;
    }

    /**
     * 获取数据类型
     * @return
     */
    public DataType getDataType() {
        return dataType;
    }

    /**
     * 设置数据类型
     * @return
     */
    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    /**
     * 获取事件类型
     * @return
     */
    public Event getEvent() {
        return event;
    }

    /**
     * 设置事件类型
     * @param event
     */
    public void setEvent(Event event) {
        this.event = event;
    }

    /**
     * 获取更新之前的数据
     * @return
     */

    public Map<String, Object> getBefore() {
        return before;
    }

    /**
     * 设置更新之前的数据
     * @param before
     */

    public void setBefore(Map<String, Object> before) {
        this.before = before;
    }
    /**
     * 获取更新之后的数据
     * @return
     */

    public Map<String, Object> getAfter() {
        return after;
    }

    /**
     * 设置更新之后的数据
     * @param after
     */

    public void setAfter(Map<String, Object> after) {
        this.after = after;
    }

    public List<String> getKeys() {
        return keys;
    }

    public void setKeys(List<String> keys) {
        this.keys = keys;
    }
}
