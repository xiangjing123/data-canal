package com.tmg.internship.datacanal.escenter.moduls.config;

/**
 * ES field 下面的子属性
 *
 * @author xiangjing
 * @date 2018/5/30
 * @company 天极云智
 */
public class PropertyNode {

    // 字段名
    private String columnName;

    //映射类型
    private String mappingType;

    //分词器
    private Tokenizer tokenizer;

    //日期格式
    private String format;
    /**
     * 是否建立doc——values 文档
     */
    private Boolean keyword;

    /**
     * 获取源数据列名
     * @return
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * 设置源数据列名
     * @param columnName
     */
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    /**
     * 获取映射类型
     * @return
     */
    public String getMappingType() {
        return mappingType;
    }

    /**
     * 设置映射类型
     * @param mappingType
     */
    public void setMappingType(String mappingType) {
        this.mappingType = mappingType;
    }

    /**
     * 获取分词器
     * @return
     */
    public Tokenizer getTokenizer() {
        return tokenizer;
    }

    /**
     * 设置分词器
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
     * 获取是否创建doc-values
     * @return
     */

    public Boolean getKeyword() {
        return keyword;
    }

    /**
     * 设置是否创建doc-values
     * @param keyword
     */
    public void setKeyword(Boolean keyword) {
        if (null == keyword) {
            this.keyword = Boolean.TRUE;
        } else {
            this.keyword = keyword;
        }
    }

    @Override
    public String toString() {
        return "{" +
                "columnName='" + columnName + '\'' +
                ", mappingType='" + mappingType + '\'' +
                ", tokenizer=" + tokenizer +
                ", format='" + format + '\'' +
                ", keyword=" + keyword +
                '}';
    }
}
