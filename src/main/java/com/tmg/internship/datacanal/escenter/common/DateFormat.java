package com.tmg.internship.datacanal.escenter.common;

/**
 * 时间格式
 *
 * @author xiangjing
 * @date 2018/5/30
 * @company 天极云智
 */
public enum  DateFormat {
    YYYYMMDD("yyyyMMdd"),
    yyyy_MM_dd("yyyy-MM-dd"),
    yyyy_MM_dd_HH_mm_ss("yyyy-MM-dd HH:mm:ss"),
    yyyy_MM_dd_HH_mm("yyyy-MM-dd HH:mm"),
    yyyy_MM_dd_HH_mm_ss_SSS("yyyy-MM-dd HH:mm:ss.SSS"),
    YYYY_MM_DD("yyyy/MM/dd"),
    YYYY_MM_DD_HH_MM_SS("yyyy/MM/dd HH:mm:ss"),
    YYYY_MM_DD_HH_MM("yyyy/MM/dd HH:mm");

    private String code;

    DateFormat(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }
}
