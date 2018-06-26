package com.tmg.internship.datacanal.escenter.trigger.function;

import com.tmg.internship.datacanal.escenter.moduls.mapping.cast.MappingType;

/**
 *
 *表合并使用的联合函数接口
 * @author xiangjing
 * @date 2018/6/12
 * @company 天极云智
 */
public interface CombineFunction<result> {

    /**
     * 获取函数的类型
     * @return
     */
    FunctionType getFunctionType();

    /**
     *函数的调用方法
     * @param params 参数
     */
    result call(Object... params);

    /**
     * 返回 数据的类型
     * @return
     */
    MappingType getType();

    /**
     * 获取能够匹配函数的正则表达式
     * @return
     */
    String getRegex();



}
