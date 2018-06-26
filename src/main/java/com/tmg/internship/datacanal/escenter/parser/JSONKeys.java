package com.tmg.internship.datacanal.escenter.parser;

/**
 * mq 数据默认keys
 *
 * @author xiangjing
 * @date 2018/5/7
 * @company 天极云智
 */
public enum JSONKeys {
    EVENT("event"),BEFORE("before"),AFTER("after"),PK("pk");

    private String code;

    JSONKeys(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }
}
