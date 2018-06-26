package com.tmg.internship.datacanal.escenter.moduls.index;

import com.tmg.internship.datacanal.escenter.moduls.config.CollectionNode;
import com.tmg.internship.datacanal.escenter.moduls.config.ConcatParam;
import com.tmg.internship.datacanal.escenter.moduls.config.MappingRule;
import com.tmg.internship.datacanal.escenter.parser.EventData;

import java.util.List;
import java.util.Map;

/**
 * MQ 数据处理接口
 *
 * @author xiangjing
 * @date 2018/5/14
 * @company 天极云智
 */
public interface MQHandleData {

    /**
     * 解析数据
     * @return
     */
    HandleMapping HandleData(EventData data);

    /**
     * 解析 规则映射 替换规则
     * @param columnValue 需要替换的值
     * @param replace 替换的规则
     * @return 返回替换过后的字符串
     */
    String replaceRule(String columnValue,MappingRule.Replace replace);

    /**
     * 解析映射规则 拼接规则
     * @param params 连接的参数
     * @param columns 源数据
     * @param fields 映射过后的数据
     * @return   返回拼接过后的字符串
     */
    String concatRule(List<ConcatParam> params,Map<String,Object> columns,Map<String,Object> fields);

    /**
     * 设置collectionNode 节点的配置
     * @param collectionNode
     */
    void setCollectionNodeSetting(CollectionNode collectionNode);
}
