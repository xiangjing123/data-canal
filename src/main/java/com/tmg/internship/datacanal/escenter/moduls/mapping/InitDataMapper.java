package com.tmg.internship.datacanal.escenter.moduls.mapping;

import com.tmg.internship.datacanal.escenter.moduls.index.MappingMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 初始化数据定义mapper用于配置mapping
 *
 * @author xiangjing
 * @date 2018/5/25
 * @company 天极云智
 */
public class InitDataMapper extends BaseMapper {

    public static final Logger logger = LoggerFactory.getLogger(InitDataMapper.class);

    public static Map<String,Object> fields =null;

    public static  String index="";
    /**
     * 构造器
     *
     * @param mappingCaster
     */
    public InitDataMapper(MappingCaster mappingCaster) {
        super(mappingCaster);
    }

    @Override
    public IndexChange checkIndexConfig(MappingMap mappingMap) throws Exception {

        indexChange = new IndexChange();
        index=mappingMap.getIndex();
        fields=mappingMap.getFields();
        Map<String, Object> oldIndex = getMappingByCache(mappingMap.getIndex());

        if (null == oldIndex) {//index 不存在
            indexChange.setChangeType(IndexChange.ChangeType.NOT_EXIST);
            return indexChange;
        }
        Map<String, Map<String, Object>> newIndex = mappingMap.getIndexMapping();
        if (encryptObject(newIndex).equals(encryptObject(oldIndex))) {//比较整体的是否改变
            indexChange.setChangeType(IndexChange.ChangeType.NO_CHANGE);
            return indexChange;
        } else {
            //比较mapping
           Map<String,Object> result=compareMapping((Map<String,Object>)oldIndex.get(ESKeyWord.Mapping.mapping.name()), newIndex.get(ESKeyWord.Mapping.mapping.name()));
            if(result.keySet().size()>0){
                indexChange.setChangeType(IndexChange.ChangeType.UPDATE);
            }else{
                indexChange.setChangeType(IndexChange.ChangeType.NO_CHANGE);
            }
        }
        return indexChange;
    }

    @Override
    protected Map<String, Object> compareMapping(Map<String, Object> oldMapping, Map<String, Object> newMapping) {
        Map<String,Object> map =new HashMap<>();
        if (!encryptObject(oldMapping).equals(encryptObject(newMapping))) {
            for (Map.Entry<String, Object> newField : newMapping.entrySet()) {
                for (Map.Entry<String, Object> oldField : oldMapping.entrySet()) {
                    if (newField.getKey().equals(oldField.getKey())) {//如果属性key相等
                        if (!encryptObject(newField.getValue()).equals(encryptObject(oldField.getValue()))) {//如果映射相等
                            map.put(newField.getKey(),newField.getValue());
                            logger.warn("索引："+index+",改变列："+newField.getKey()+",{},"+newField.getValue()+",参数:"+fields.get(newField.getKey()));
                        }
                    }
                }
            }
        }
        return map;
    }
}
