package com.tmg.internship.datacanal.escenter.trigger.function;

import com.tmg.internship.datacanal.escenter.exception.FunctionException;
import com.tmg.internship.datacanal.escenter.moduls.mapping.cast.MappingType;
import com.tmg.utils.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 函数策略
 *
 * @author xiangjing
 * @date 2018/6/12
 * @company 天极云智
 */
public class FunctionStrategy {

    /**
     * 函数表达式
     */
    private String function;

    /**
     * 函数
     */
    protected CombineFunction combineFunction;

    public FunctionStrategy(String function) {
        this.function = function;
        this.combineFunction =getCombineFunction(function);
    }

    /**
     * 判断函数是不是为空
     * @return
     */
    public  Boolean isFunction() {
        return null != this.combineFunction;
    }

    /**
     * 调用相应的函数，并执行相关函数
     * @param rows es的多行数据
     * @return
     */
    public Object executerFunction(List<Map<String,Object>> rows) {
        if (!isFunction()) {//判断是不是函数
            throw new FunctionException(this.function + "函数 未定义");
        } else {
            if(this.combineFunction instanceof  MapConcatFunction){
                return this.combineFunction.call(rows,getMapConcatQuery());
            }else{
                return null;
            }
        }
    }

    /**
     * 判断函数是不是函数
     *@param function 函数表达式
     * @return
     */
    public static Boolean isFunction(String function) {
        return null != getCombineFunction(function);
    }

    /**
     * 获取函数类型的code
     * @return
     */
    public String getFunctionCode() {
        if( null !=this.combineFunction ){
            return this.combineFunction.getFunctionType().getCode();
        }
        return null;
    }

    /**
     * 获取函数类型
     * @param functionCode 根据函数的枚举类型获取其类型
     * @return
     */
    public static MappingType getMappintType(String functionCode) {
       if(!StringUtils.isEmpty(functionCode)){
           FunctionType type = null;
            for(int i=0;i<FunctionType.values().length;i++){
                type = FunctionType.values()[i];
                if(type.getCode().equals(functionCode)){
                    return getMappintType(type);
                }
            }
       }
        return null;
    }

    /**
     * 根据函数表达式 获取函数
     * @param type 函数的枚举类型
     * @return
     */
    public static MappingType getMappintType(FunctionType type) {
        switch (type){
            case MAP_CONCAT:
                return new MapConcatFunction().getType();
            default:
                return null;
        }
    }

    /**
     * 根据函数 能够匹配的正则表达式
     * @param type 函数的枚举类型
     * @return
     */
    public static String getFunctionRegex(FunctionType type) {
        switch (type){
            case MAP_CONCAT:
                return new MapConcatFunction().getRegex();
            default:
                return null;
        }
    }

    /**
     * 根据函数表达式 获取函数
     * @param function
     * @return
     */
    protected static  CombineFunction getCombineFunction(String function) {

        if(StringUtils.isEmpty(function)){
            return null;
        }
        if (MapConcatFunction.checkFunctionType(function)) {//如果不是map_concat函数
            return new MapConcatFunction();
        } else {
            return null;
        }
    }

    /**
     * 获取map_concat 函数的key和value
     * @return
     */
    private Map<String,String> getMapConcatQuery(){
        String param= this.function.substring(function.indexOf("(")+1,function.indexOf(")"));
        String[] keyValue =param.split(",");
        Map<String,String> concat = new HashMap<>();
        concat.put(keyValue[0],keyValue[1]);
        return concat;
    }

    public String getFunction() {
        return function;
    }

    public void setFunction(String function) {
        this.function = function;
        this.combineFunction =getCombineFunction(function);
    }
}
