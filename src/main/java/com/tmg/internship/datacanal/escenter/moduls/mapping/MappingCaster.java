package com.tmg.internship.datacanal.escenter.moduls.mapping;

import com.tmg.internship.datacanal.escenter.exception.MappingException;
import com.tmg.internship.datacanal.escenter.moduls.config.CollectionNode;
import com.tmg.internship.datacanal.escenter.moduls.config.ESConfigure;
import com.tmg.internship.datacanal.escenter.moduls.config.MappingNode;
import com.tmg.internship.datacanal.escenter.moduls.config.Tokenizer;
import com.tmg.internship.datacanal.escenter.moduls.index.MappingMap;
import com.tmg.internship.datacanal.escenter.moduls.mapping.cast.MappingType;

import java.util.Map;

/**
 * Mapping 创造接口
 *
 * @author xiangjing
 * @date 2018/5/9
 * @company 天极云智
 */
public interface MappingCaster {

    /**
     * 创建索引的映射数据
     * @param map
     * @return
     * @throws MappingException
     */
    Map<String,Object> createIndexMaping(MappingMap map) throws MappingException;

    /**
     * 创建索引的配置
     * @param  map
     * @return
     * @throws MappingException
     */
    Map<String,Object> createIndexsettings(MappingMap map) throws MappingException;

    /**
     * 创建索引的别名
     * @param map
     * @return
     * @throws MappingException
     */
    Map<String,Object> createAliases(MappingMap map) throws MappingException;

    /**
     * 设置mappingNode 节点的配置
     * @param mappingNode
     */
    void setMappingNodeSetting(MappingNode mappingNode);

    /**
     * 设置collectionNode 节点的配置
     * @param collectionNode
     */
    void setCollectionNodeSetting(CollectionNode collectionNode);
    /**
     * 设置配置文件
     * @param esConfigure
     */
    void setESConfigure(ESConfigure esConfigure);

    /**
     * 默认类型映射
     * @param value
     * @return
     */
    MappingType defaultType(Object value);

    /**
     * 类型约定
     * @param key
     * @param value
     * @return
     */
    MappingType typeAppoint(String key,Object value);
    /**
     * 分词约定
     * @param key
     * @return
     */
    Tokenizer tokenizerAppoint(String key);

}
