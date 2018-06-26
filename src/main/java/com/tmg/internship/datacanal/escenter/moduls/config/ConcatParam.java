package com.tmg.internship.datacanal.escenter.moduls.config;

/**
 * 配置文件映射规则连接里面的参数类型
 *
 * @author xiangjing
 * @date 2018/5/11
 * @company 天极云智
 */
public class ConcatParam {
    /**
     * 映射规则评价 参数类型
     */
    private ParamType type;

    /**
     * 映射规则评价 参数
     */
    private String param;

    /**
     * 设置值
     * @param type
     * @param param
     */
    public ConcatParam(ConcatParam.ParamType type,Object param){
        this.type =type;
        if(null == param){
            this.param ="";
        }else{
            this.param = param.toString();
        }
    }

    public ConcatParam() {
    }

    public enum ParamType {
        COMMON,COLUMN,FIELD
    }

    public ParamType getType() {
        return type;
    }

    public void setType(ParamType type) {
        this.type = type;
    }

    public String getParam() {
        return param;
    }

    public void setParam(String param) {
        this.param = param;
    }

    @Override
    public String toString() {
        return "ConcatParam{" +
                "type=" + type +
                ", param='" + param + '\'' +
                '}';
    }
}
