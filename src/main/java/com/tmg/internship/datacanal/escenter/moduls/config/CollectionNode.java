package com.tmg.internship.datacanal.escenter.moduls.config;

import com.tmg.internship.datacanal.escenter.common.BaseClass;
import com.tmg.internship.datacanal.escenter.moduls.config.event.Trigger;
import com.tmg.internship.datacanal.escenter.parser.Event;
import com.tmg.utils.StringUtils;

import java.util.List;

/**
 * collection 用于定义数据域下的同类数据集，如：数据库表，日志文件等
 *
 * @author xiangjing
 * @date 2018/5/11
 * @company 天极云智
 */
public class CollectionNode extends BaseClass {

    private String name;//数据集名称

    private String indexName;//设置的索引名称，如果没设置，则使用数据集的名称

    private List<FieldNode> fieldNodeList;//设置数据集下面 字段属性配置

    private List<Trigger> triggers;//对应配置文件trggers 节点，设置触发条件

    //默认主分片数
    private Integer numberOfShards;

    //默认副分片数
    private Integer numberOfReplicas;

    /**
     * 获取数据集的名称
     *
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     * 设置数据集的名称
     *
     * @param name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * 获取对应ES 要设置的索引名称
     *
     * @return
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * 设置索引名称，如果索引名字为空则默认使用数据集的名称
     *
     * @param indexName
     */
    public void setIndexName(String indexName) {
        if (StringUtils.isTrimEmpty(indexName)) {
            this.indexName = this.name;
        } else {
            this.indexName = indexName;
        }

    }

    /**
     * 获取数据集下面所有字段的配置
     *
     * @return
     */
    public List<FieldNode> getFieldNodeList() {
        return fieldNodeList;
    }

    /**
     * 设置数据集下面配置了的filed 属性
     *
     * @param fieldNodeList
     */
    public void setFieldNodeList(List<FieldNode> fieldNodeList) {
        this.fieldNodeList = fieldNodeList;
    }

    /**
     * 获取触发配置
     *
     * @return
     */
    public List<Trigger> getTriggers() {
        return triggers;
    }

    /**
     * 设置触发配置
     *
     * @param triggers
     */
    public void setTriggers(List<Trigger> triggers) {
        this.triggers = triggers;
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

    /**
     * 根据源数据 列名寻找相关配置
     *
     * @param columnName
     * @return
     */
    public FieldNode getFieldNodeByColumn(String columnName) {

        if (null == this.fieldNodeList || this.fieldNodeList.size() == 0) {
            return null;
        }
        for (FieldNode fieldNode : this.fieldNodeList) {
            if (columnName.equals(fieldNode.getColumnName())) {
                return fieldNode;
            }
        }
        return null;
    }

    /**
     * 根据ES 映射的属性名寻找相关配置
     *
     * @param fieldName
     * @return
     */
    public FieldNode getFieldNodeByField(String fieldName) {

        if (null == this.fieldNodeList || this.fieldNodeList.size() == 0) {
            return null;
        }
        for (FieldNode fieldNode : this.fieldNodeList) {
            if (StringUtils.isTrimEmpty(fieldNode.getFiledName())) {
                if (fieldName.equals(fieldNode.getColumnName())) {
                    return fieldNode;
                }
            } else {
                if (fieldName.equals(fieldNode.getFiledName())) {
                    return fieldNode;
                }
            }
        }
        return null;
    }

    /**
     * 根据操作类型
     *
     * @param type
     * @return
     */
    public Trigger getTrigger(Event type) {

        if (null != this.triggers && this.triggers.size() != 0) {
            for(Trigger trigger:this.triggers){
                if( trigger.getEvent() == type){
                    return trigger;
                }
            }
        }
        return null;

    }

    @Override
    public String toString() {
        return "{" +
                "name='" + name + '\'' +
                ", indexName='" + indexName + '\'' +
                ", numberOfShards=" + numberOfShards +
                ", numberOfReplicas=" + numberOfReplicas +
                ", fieldNodeList=" + fieldNodeList +
                ", triggers=" + triggers +
                '}';
    }
}
