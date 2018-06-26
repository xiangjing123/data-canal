package com.tmg.internship.datacanal.escenter.moduls.config;

import com.tmg.internship.datacanal.escenter.exception.ESException;

/**
 * 配置解析器
 *
 * @author xiangjing
 * @date 2018/5/8
 * @company 天极云智
 */
public interface ConfigParser {
    /**
     * 加载配置文件
     * @return
     * @throws ESException
     */
    ESConfigure parseConfig() throws Exception;

}
