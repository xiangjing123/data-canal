package com.tmg.internship.datacanal.escenter.moduls.config;

import com.tmg.internship.datacanal.escenter.common.BaseClass;
import java.util.List;

/**
 * 映射规则
 *
 * @author xiangjing
 * @date 2018/5/10
 * @company 天极云智
 */
public class MappingRule  extends BaseClass {

    //替换规则
    private List<Replace> replaceAll;

    /**
     * 字符串拼接条件
     */
    private List<ConcatParam> concat;

    /**
     * 无参构造器
     */
    public MappingRule() {
    }

    /**
     * 获取所有的替换规则
     * @return
     */
    public List<Replace> getReplaceAll() {
        return replaceAll;
    }

    /**
     * 设置替换规则
     * @param replaceAll
     */
    public void setReplaceAll(List<Replace> replaceAll) {
        this.replaceAll = replaceAll;
    }

    public List<ConcatParam> getConcat() {
        return concat;
    }

    public void setConcat(List<ConcatParam> concat) {
        this.concat = concat;
    }


    public class Replace{
        //正则
        private String regex;
        //替换的字符串
        private String replacement;

        public String getRegex() {
            return regex;
        }

        public void setRegex(String regex) {
            this.regex = regex;
        }

        public String getReplacement() {
            return replacement;
        }

        public void setReplacement(String replacement) {
            this.replacement = replacement;
        }

        @Override
        public String toString() {
            return "replace{" +
                    "regex='" + regex + '\'' +
                    ", replacement='" + replacement + '\'' +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "{" +
                "replaceAll=" + replaceAll +
                ", concat=" + concat +
                '}';
    }
}
