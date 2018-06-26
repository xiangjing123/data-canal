package com.tmg.internship.datacanal.escenter.moduls.config.event;

import com.tmg.internship.datacanal.escenter.parser.Event;

import java.util.List;

/**
 * 需要进行级联更新的操作
 *
 * @author xiangjing
 * @date 2018/5/11
 * @company 天极云智
 */
public class EventHandle {

    private Event event;//操作类型

    private String indexName;//索引名称 用于合并的时候使用

    //默认主分片数
    private Integer numberOfShards;

    //默认副分片数
    private Integer numberOfReplicas;

    private String filed;// 要更新的索引名及字段名

    private String value;// 要级联更新的具体值，可不配置或留空，默认使用columnName的值

    private List<Condition> conditionList;//查询条件

    private List<SourceNode> sourceNodeList;//合并索引列


    public Event getEvent() {
        return event;
    }

    public void setEvent(Event event) {
        this.event = event;
    }

    public String getFiled() {
        return filed;
    }

    public void setFiled(String filed) {
        this.filed = filed;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public List<Condition> getConditionList() {
        return conditionList;
    }

    public void setConditionList(List<Condition> conditionList) {
        this.conditionList = conditionList;
    }

    public List<SourceNode> getSourceNodeList() {
        return sourceNodeList;
    }

    public void setSourceNodeList(List<SourceNode> sourceNodeList) {
        this.sourceNodeList = sourceNodeList;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }


    /**
     * 获取主分片数
     * @return
     */
    public Integer getNumberOfShards() {
        return numberOfShards;
    }

    /**
     * 设置主分片数
     * @param numberOfShards
     */
    public void setNumberOfShards(Integer numberOfShards) {
        this.numberOfShards = numberOfShards;
    }

    /**
     * 获取副分片数
     * @return
     */
    public Integer getNumberOfReplicas() {
        return numberOfReplicas;
    }

    /**
     * 设置副分片数
     * @param numberOfReplicas
     */
    public void setNumberOfReplicas(Integer numberOfReplicas) {
        this.numberOfReplicas = numberOfReplicas;
    }

    @Override
    public String toString() {
        return "{" +
                "event=" + event +
                ", indexName='" + indexName + '\'' +
                ", numberOfShards=" + numberOfShards +
                ", numberOfReplicas=" + numberOfReplicas +
                ", filed='" + filed + '\'' +
                ", value='" + value + '\'' +
                ", conditionList=" + conditionList +
                ", sourceNodeList=" + sourceNodeList +
                '}';
    }
}
