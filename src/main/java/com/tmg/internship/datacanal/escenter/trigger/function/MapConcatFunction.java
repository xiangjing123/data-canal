package com.tmg.internship.datacanal.escenter.trigger.function;

import com.tmg.internship.datacanal.escenter.exception.FunctionException;
import com.tmg.internship.datacanal.escenter.moduls.mapping.cast.MappingType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 将多条数据合并成一条数据的函数
 *
 * @author xiangjing
 * @date 2018/6/12
 * @company 天极云智
 */
public class MapConcatFunction implements CombineFunction<Map<Object,Object>> {

    @Override
    public FunctionType getFunctionType() {
        return FunctionType.MAP_CONCAT;
    }

    /**
     * 校验表达式是不是函数
     * @param function 函数表达式
     * @return
     */
    public static Boolean checkFunctionType(String function) {
        String regex ="^"+FunctionType.MAP_CONCAT.getCode()+"\\([_\\dA-Za-z]+,[_\\dA-Za-z]+\\)$";
        return function.matches(regex);
    }


    /**
     * 第一个参数是索引
     * 第二个参数是:ES查询条件
     * 第三个参数是:合并条件
     * @param params 参数
     * @return
     */
    @Override
    public Map<Object, Object> call(Object... params) {
        try {
            List<Map<String,Object>> rows = (List<Map<String,Object>>)params[0];//多行数据
            Map<String,String> keyValue = (Map<String, String>)params[1];//要合并的key 对应的value
            String key =keyValue.keySet().iterator().next();
            String value =keyValue.get(key);
            Object keyVal= null;
            Map<Object,Object> result =new HashMap<>();
            for (Map<String, Object> row:rows){
                keyVal = row.get(key);
                if(null != keyVal ){
                    result.put(keyVal,row.get(value));
                }
            }
            return result;

        } catch (Exception e) {
            if (e instanceof IndexOutOfBoundsException) {//数组下标越界
                throw  new FunctionException(getFunctionType()+",函数 参数不能为空",e);
            }else if( e instanceof  ClassCastException){//类型转换错误
                throw  new FunctionException(getFunctionType()+",函数 参数类型错误，need List<Map<String,Object>>",e);
            }
            throw  new FunctionException("map_concat 函数 合并失败！",e);
        }
    }

    @Override
    public MappingType getType() {
        return MappingType.NESTED;
    }

    @Override
    public String getRegex() {
        return FunctionType.MAP_CONCAT.getCode()+"\\([_\\dA-Za-z]+,[_\\dA-Za-z]+\\)";
    }


}
