package com.tmg.internship.datacanal.escenter.moduls.config;

import com.tmg.internship.datacanal.escenter.common.BaseClass;
import com.tmg.utils.StringUtils;

import java.util.List;

/**
 * 基本类型
 *
 * @author xiangjing
 * @date 2018/5/8
 * @company 天极云智
 */
public class FieldNode extends BaseClass {
    // 字段名
    private String columnName;

    //映射名
    private String filedName;

    //映射类型
    private String mappingType;

    //分词器
    private Tokenizer tokenizer;

    //日期格式
    private String format;

    //是否使用该属性进行排序聚合脚本查询 默认值为false
    private Boolean fieldData=Boolean.FALSE;

    //字段子域设置
    private List<SubField> subFieldList;

    //针对json格式的数据是否对其子属性进行设置
    private List<PropertyNode> propertyNodeList;

    /**
     * 是否建立doc——values 文档
     */
    private Boolean keyword;

    /**
     * 数据映射到ES 的规则
     */
    private MappingRule mappingRule;

    /**
     * 获取字段名
     *
     * @return
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * 设置字段名
     *
     * @param columnName
     */
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    /**
     * 获取映射名
     *
     * @return
     */
    public String getFiledName() {
        return filedName;
    }

    /**
     * 设置映射名
     *
     * @param filedName
     */
    public void setFiledName(String filedName) {
        if (StringUtils.isTrimEmpty(filedName)) {
            this.filedName = this.columnName;
        } else {
            this.filedName = filedName;
        }

    }

    /**
     * 获取映射类型
     *
     * @return
     */
    public String getMappingType() {
        return mappingType;
    }

    /**
     * 设置映射类型
     *
     * @param mappingType
     */
    public void setMappingType(String mappingType) {
        this.mappingType = mappingType;
    }

    /**
     * 获取分词器
     *
     * @return
     */
    public Tokenizer getTokenizer() {
        return tokenizer;
    }

    /**
     * 设置分词器
     *
     * @param tokenizer
     */
    public void setTokenizer(Tokenizer tokenizer) {
        this.tokenizer = tokenizer;

    }

    /**
     * 获取时间格式
     * @return
     */
    public String getFormat() {
        return format;
    }

    /**
     * 设置时间格式
     * @param format
     */
    public void setFormat(String format) {
        this.format = format;
    }

    /**
     * 获取 fieldData 参数
     * @return
     */
    public Boolean getFieldData() {
        return fieldData;
    }

    /**
     * 设置fieldData 如果为空 则设置为FALSE
     * @param fieldData
     */
    public void setFieldData(Boolean fieldData) {
        if( null !=fieldData){
            this.fieldData = fieldData;
        }else{
            this.fieldData = Boolean.FALSE;
        }

    }

    /**
     * 获取字段子域的配置
     * @return
     */
    public List<SubField> getSubFieldList() {
        return subFieldList;
    }

    /**
     * 设置字段子域的配置
     * @param subFieldList
     */
    public void setSubFieldList(List<SubField> subFieldList) {
        this.subFieldList = subFieldList;
    }

    /**
     * 设置对象子属性的配置(注：针对nested 类型)
     * @return
     */
    public List<PropertyNode> getPropertyNodeList() {
        return propertyNodeList;
    }

    /**
     * 获取对象子属性的配置(注：针对nested 类型)
     * @param propertyNodeList
     */
    public void setPropertyNodeList(List<PropertyNode> propertyNodeList) {
        this.propertyNodeList = propertyNodeList;
    }

    /**
     * 获取keyword
     *
     * @return
     */
    public Boolean getKeyword() {
        return keyword;
    }

    /**
     * 设置 keyword
     *
     * @param keyword
     */
    public void setKeyword(Boolean keyword) {
        if (null == keyword) {
            this.keyword = Boolean.TRUE;
        } else {
            this.keyword = keyword;
        }

    }

    /**
     * 获取数据映射到ES 的映射规则
     *
     * @return
     */
    public MappingRule getMappingRule() {
        return mappingRule;
    }

    /**
     * 设置数据映射到ES 的映射规则
     *
     * @param mappingRule
     */
    public void setMappingRule(MappingRule mappingRule) {
        this.mappingRule = mappingRule;
    }

    /**
     * 根据json 对象里面子属性列名获取配置
     * @param propertyName
     * @return
     */
    public PropertyNode getPropertyByName(String propertyName){
        if(StringUtils.isTrimEmpty(propertyName)){
            return null;
        }
        if( null != propertyNodeList && propertyNodeList.size()!=0 ){
            for(PropertyNode propertyNode:this.propertyNodeList){
                if(propertyNode.getColumnName().equals(propertyName)){
                    return propertyNode;
                }
            }

        }
        return null;
    }

    @Override
    public String toString() {
        return "FieldNode{" +
                "columnName='" + columnName + '\'' +
                ", filedName='" + filedName + '\'' +
                ", mappingType='" + mappingType + '\'' +
                ", tokenizer=" + tokenizer +
                ", format='" + format + '\'' +
                ", fieldData=" + fieldData +
                ", subFieldList=" + subFieldList +
                ", propertyNodeList=" + propertyNodeList +
                ", keyword=" + keyword +
                ", mappingRule=" + mappingRule +
                '}';
    }
}
