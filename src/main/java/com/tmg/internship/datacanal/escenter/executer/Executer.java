package com.tmg.internship.datacanal.escenter.executer;

import com.tmg.internship.datacanal.escenter.exception.ExecuteException;
import com.tmg.internship.datacanal.escenter.parser.EventData;

/**
 * 执行器模块
 *
 * @author xiangjing
 * @date 2018/5/5
 * @company 天极云智
 */
public interface Executer {

    /**
     * 执行器
     * @param eventData
     * @throws ExecuteException
     */
    void execute(EventData eventData) throws Exception;
}
