package com.tmg.internship.datacanal.escenter.moduls.index;

import com.tmg.internship.datacanal.escenter.parser.EventData;

import java.io.Serializable;
import java.util.Map;

/**
 * 映射内容, map 包含了一个完整的映射
 *
 * @author xiangjing
 * @date 2018/5/8
 * @company 天极云智
 */
public class MappingMap implements Serializable{

    private static final long serialVersionUID = -7029453318721514995L;

    /**
     * 默认文档类型
     */
    public static String default_doc ="doc";

    //对应ES 索引
    private String index;

    //对应ES 类型
    private String type=default_doc;

    //对应ES id
    private String id;

    //原始数据
    private EventData data;

    //对应ES 映射的完整列数据（可以直接插入到es）
    private Map<String,Object> fields;

    //columnName 映射 fieldName，example:{columnName:fieldName}
    private Map<String,String> colRelationField;

    //数据更新对应的映射
    private Map<String,Object> updateFields;

    //ES 索引里面的映射
    private Map<String,Map<String,Object>> indexMapping;

    /**
     * 获取索引名
     * @return
     */
    public String getIndex() {
        return index;
    }

    /**
     *设置索引名
     * @param index
     */
    public void setIndex(String index) {
        this.index = index;
    }

    /**
     * 获取文档类型，默认使用doc
     * @return
     */
    public String getType() {
        return type;
    }

    /**
     * 设置文档类型
     * @param type
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * 获取ES id
     * @return
     */
    public String getId() {
        return id;
    }

    /**
     * 设置ES id
     * @param id
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * 获取数据处理之前的原始数据
     * @return
     */
    public EventData getData() {
        return data;
    }

    /**
     * 设置数据处理之前的原始数据
     * @param data
     */
    public void setData(EventData data) {
        this.data = data;
    }

    /**
     * 获取完整映射
     * @return
     */
    public Map<String, Object> getFields() {
        return fields;
    }

    /**
     * 设置完整映射
     * @param fields
     */
    public void setFields(Map<String, Object> fields) {
        this.fields = fields;
    }

    /**
     * 获取columnName 和fieldName 的映射关系
     * @return
     */
    public Map<String, String> getColRelationField() {
        return colRelationField;
    }

    /**
     * 设置 columnName 和fieldName 的映射关系
     * @param colRelationField
     */
    public void setColRelationField(Map<String, String> colRelationField) {
        this.colRelationField = colRelationField;
    }

    /**获取修改事件的局部映射
     *
     * @return
     */
    public Map<String, Object> getUpdateFields() {
        return updateFields;
    }

    /**
     * 设置修改事件的局部映射
     * @param updateFields
     */
    public void setUpdateFields(Map<String, Object> updateFields) {
        this.updateFields = updateFields;
    }

    /**
     * 获取索引映射，配置，别名
     * @return
     */
    public Map<String, Map<String, Object>> getIndexMapping() {
        return indexMapping;
    }

    /**
     * 设置索引映射，配置，别名
     * @return
     */
    public void setIndexMapping(Map<String, Map<String, Object>> indexMapping) {
        this.indexMapping = indexMapping;
    }

    @Override
    public String toString() {
        return "MappingMap{" +
                "index='" + index + '\'' +
                ", type='" + type + '\'' +
                ", id='" + id + '\'' +
                ", fields=" + fields +
                ", updateFields=" + updateFields +
                ", indexMapping=" + indexMapping +
                '}';
    }
}
