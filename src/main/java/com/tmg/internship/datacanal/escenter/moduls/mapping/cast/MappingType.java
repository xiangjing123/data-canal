package com.tmg.internship.datacanal.escenter.moduls.mapping.cast;

/**
 * 定义基本的映射类型
 *
 * @author xiangjing
 * @date 2018/5/9
 * @company 天极云智
 */
public enum MappingType {
    //定义ES 类型
    NESTED("nested"),
    TEXT("text"),
    KEYWORD("keyword"),
    LONG("long"),
    DOUBLE("double"),
    BOOLEAN("boolean"),
    FLOAT("float"),
    SHORT("short"),
    INTEGER("integer"),
    DATE("date");

    private String code;

    MappingType(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }
}
