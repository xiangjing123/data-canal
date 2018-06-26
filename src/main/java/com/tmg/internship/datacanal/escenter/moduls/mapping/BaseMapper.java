package com.tmg.internship.datacanal.escenter.moduls.mapping;

import com.tmg.internship.datacanal.escenter.exception.MappingException;
import com.tmg.internship.datacanal.escenter.moduls.index.HandleMapping;
import java.util.Map;
import java.util.TreeMap;

/**
 * 基本表结构映射
 *
 * @author xiangjing
 * @date 2018/5/8
 * @company 天极云智
 */
public class BaseMapper extends  AbstractDefaultMapper{

    /**
     * 映射铸造器
     */
    private MappingCaster mappingCaster;

    /**
     * 无参构造器
     */
    public BaseMapper() {
    }

    /**
     * 构造器
     * @param mappingCaster
     */
    public BaseMapper(MappingCaster mappingCaster) {
        this.mappingCaster = mappingCaster;
    }

    @Override
    public void parseMapping(HandleMapping data) throws MappingException {
        Map<String,Map<String,Object>> mapping = new TreeMap<>();
        mapping.put(ESKeyWord.Mapping.mapping.name(),mappingCaster.createIndexMaping(data));//未添加到properties 节点和type节点
        mapping.put(ESKeyWord.aliases.aliases.name(),mappingCaster.createAliases(data));
        Map<String,Object> settings =mappingCaster.createIndexsettings(data);
        if( null != settings){
            mapping.put(ESKeyWord.Setting.settings.name(),settings);
        }
        data.setIndexMapping(mapping);
    }

    @Override
    public void setMappingCaster(MappingCaster caster) {
        this.mappingCaster =caster;
    }
}
