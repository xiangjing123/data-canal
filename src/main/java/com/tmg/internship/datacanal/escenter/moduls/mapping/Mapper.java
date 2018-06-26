package com.tmg.internship.datacanal.escenter.moduls.mapping;

import com.tmg.internship.datacanal.escenter.exception.MappingException;
import com.tmg.internship.datacanal.escenter.moduls.index.HandleMapping;
import com.tmg.internship.datacanal.escenter.moduls.index.MappingMap;
import java.util.Map;

/**
 * 映射器
 *
 * @author xiangjing
 * @date 2018/5/8
 * @company 天极云智
 */
public interface Mapper {

    /**
     * 对数据进行解析,生成映射格式的字符串
     * @param data 解析过后的参数
     * @return
     * @throws MappingException
     */
    void parseMapping(HandleMapping data) throws MappingException;

    /**
     * 校验ES 映射
     * @param mappingMap 数据
     * @return
     */
    IndexChange checkIndexConfig(MappingMap mappingMap) throws Exception;

    /**
     * 缓存mapping
     * @param map
     * @throws MappingException
     */
    void cacheMap(MappingMap map) throws Exception;

    /**
     * 根据索引 获取缓存里面的映射
     * @param index
     * @return
     */
    Map<String,Object> getMappingByCache(String index) throws Exception;

    /**
     * 设置mapping 构造器
     * @param caster
     */
    void setMappingCaster(MappingCaster caster);

}
