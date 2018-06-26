package com.tmg.internship.datacanal.escenter.trigger;

import com.tmg.internship.datacanal.escenter.exception.ESException;
import com.tmg.internship.datacanal.escenter.moduls.config.MappingNode;
import com.tmg.internship.datacanal.escenter.moduls.index.MappingMap;

/**
 * es 触发器
 *
 * @author xiangjing
 * @date 2018/6/1
 * @company 天极云智
 */
public interface ESTrigger {

    /**
     * 定义的事件触发器，用于触发各种事件
     * @param mappingSettings 相应的mapping 配置
     * @param data 数据
     * @throws ESException
     */
    void eventTrigger(MappingNode mappingSettings,MappingMap data) throws ESException, InterruptedException;

}
