package com.tmg.internship.datacanal.escenter.executer;

import com.tmg.internship.datacanal.escenter.moduls.index.HandleMapping;

/**
 * ES 中心操作接口
 *
 * @author xiangjing
 * @date 2018/6/11
 * @company 天极云智
 */
public interface ESAction {

    /**
     * 根据参数将数据push到ES
     * @param handleMap
     */
    void doAction(HandleMapping handleMap) throws Exception;

}
