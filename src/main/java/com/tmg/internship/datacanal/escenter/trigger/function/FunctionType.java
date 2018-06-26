package com.tmg.internship.datacanal.escenter.trigger.function;

/**
 * 合并的函数类型
 * @author xiangjing
 * @date 2018/6/12
 * @company 天极云智
 */
public enum FunctionType {
    //将多行key-value结构的数据整合得到对应的json对象字符串
    MAP_CONCAT("map_concat");

    private String code;

    FunctionType(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }
}
