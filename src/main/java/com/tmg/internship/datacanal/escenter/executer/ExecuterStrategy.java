package com.tmg.internship.datacanal.escenter.executer;

import com.tmg.internship.datacanal.escenter.moduls.config.ESConfigure;

/**
 * 执行器策略
 *
 * @author xiangjing
 * @date 2018/5/7
 * @company 天极云智
 */
public class ExecuterStrategy {

    private static  Executer executer;

    public static Executer  getExecuter(ESConfigure configure) {
        if( null ==executer){
            executer =new BaseExecuter(configure);
        }
        return executer;
    }
}
