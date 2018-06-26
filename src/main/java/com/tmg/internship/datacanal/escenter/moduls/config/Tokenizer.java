package com.tmg.internship.datacanal.escenter.moduls.config;

/**
 * 分词器类型
 *
 * @author xiangjing
 * @date 2018/5/8
 * @company 天极云智
 */
public enum Tokenizer {

    STANDARD("standard",Analyzer.ANALYER),//标准英文分词
    ENGLISH("english",Analyzer.ANALYER),//英文分词
    PATH("path_hierarchy",Analyzer.TOKENIZER),//路径分词
    IK_SMART("ik_smart",Analyzer.ANALYER),//中文分词（按词义）
    IK_MAX_WORD("ik_max_word",Analyzer.ANALYER);//中文分词(不按词义)

    private String code;

    private Analyzer type;

    Tokenizer(String code, Analyzer type) {
        this.code = code;
        this.type = type;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public Analyzer getType() {
        return type;
    }

    public void setType(Analyzer type) {
        this.type = type;
    }
}
