package com.tmg.internship.datacanal.escenter.moduls.config;

import com.tmg.internship.datacanal.escenter.common.BaseClass;


/**
 * 字段的子域映射
 *
 * @author xiangjing
 * @date 2018/5/11
 * @company 天极云智
 */
public class SubField extends BaseClass {

    //映射名
    private String filedName;

    //映射类型
    private String mappingType;

    //分词器
    private Tokenizer tokenizer;

    /**
     * 获取 es 子域的属性名
     * @return
     */
    public String getFiledName() {
        return filedName;
    }

    /**
     * 设置 es子域的属性名
     * @param filedName
     */
    public void setFiledName(String filedName) {
        this.filedName = filedName;
    }

    /**
     * 获取 es 子域的属性类型
     * @return
     */
    public String getMappingType() {
        return mappingType;
    }

    /**
     * 设置 es 子域的属性类型
     * @param mappingType
     */
    public void setMappingType(String mappingType) {
        this.mappingType = mappingType;
    }

    /**
     * 获取 es 子域的属性的分词类型
     * @return
     */
    public Tokenizer getTokenizer() {
        return tokenizer;
    }

    /**
     * 设置 es 子域的属性的分词类型
     * @param tokenizer
     */
    public void setTokenizer(Tokenizer tokenizer) {
        this.tokenizer = tokenizer;
    }

    @Override
    public String toString() {
        return "{" +
                "filedName='" + filedName + '\'' +
                ", mappingType='" + mappingType + '\'' +
                ", tokenizer=" + tokenizer +
                '}';
    }
}
