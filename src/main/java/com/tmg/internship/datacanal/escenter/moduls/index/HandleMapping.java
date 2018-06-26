package com.tmg.internship.datacanal.escenter.moduls.index;

import com.tmg.internship.datacanal.escenter.parser.Event;

/**
 * ES 功能模块 映射
 *
 * @author xiangjing
 * @date 2018/5/8
 * @company 天极云智
 */
public class HandleMapping extends MappingMap {


    //操作类型
    private Event handleType;

    /**
     * 获取操作类型
     * @return
     */
    public Event getHandleType() {
        return handleType;
    }

    /**
     * 设置操作类型
     * @param handleType
     */
    public void setHandleType(Event handleType) {
        this.handleType = handleType;
    }

}
