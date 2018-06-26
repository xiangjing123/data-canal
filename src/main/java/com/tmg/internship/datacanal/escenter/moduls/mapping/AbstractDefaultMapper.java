package com.tmg.internship.datacanal.escenter.moduls.mapping;

import com.alibaba.fastjson.JSON;
import com.tmg.internship.datacanal.escenter.exception.MappingException;
import com.tmg.internship.datacanal.escenter.moduls.index.MappingMap;
import com.tmg.utils.MD5;
import com.tmg.utils.redis.SimpleRedisTool;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;

/**
 * 默认映射器
 *
 * @author xiangjing
 * @date 2018/5/8
 * @company 天极云智
 */
public abstract class AbstractDefaultMapper implements Mapper {


    /**
     * 获取redis 操作实例
     */
    public static final SimpleRedisTool redis = new SimpleRedisTool();

    /**
     * 定义一个全局的类型
     */
    protected static IndexChange indexChange = null;


    /**
     * 校验 索引是否一致
     *
     * @param mappingMap
     * @return
     * @throws Exception
     */
    @Override
    public IndexChange checkIndexConfig(MappingMap mappingMap) throws Exception {

        indexChange = new IndexChange();

        Map<String, Object> oldIndex = getMappingByCache(mappingMap.getIndex());

        //旧的配置
        if (null == oldIndex) {//index 不存在
            indexChange.setChangeType(IndexChange.ChangeType.NOT_EXIST);
            indexChange.setShardChange(false);//shards 未发生变化
            return indexChange;
        }

        // 新的配置
        Map<String, Map<String, Object>> newIndex = mappingMap.getIndexMapping();

        if (encryptObject(newIndex).equals(encryptObject(oldIndex))) {//比较整体的是否改变
            indexChange.setChangeType(IndexChange.ChangeType.NO_CHANGE);
            indexChange.setShardChange(false);//shards 未发生变化
            return indexChange;
        } else {

            Map<String,Object> oldSettings=(Map<String,Object>)oldIndex.get(ESKeyWord.Setting.settings.name());
            Map<String,Object> newSettings= newIndex.get(ESKeyWord.Setting.settings.name());

            //比较分片 查看分片是否改变
            indexChange.setShardChange(compareShards(oldSettings,newSettings));

            //比较分词器
            Map<String, Map<String, Object>> changes = new HashMap<>();
            Map<String, Object> settings = compareAnalysis(oldSettings, newSettings);
            Object settingType = settings.get(ESKeyWord.Setting.settings.name());
            if (null != settingType) {//
                if (IndexChange.ChangeType.UPDATE == settingType) {//需要更新配置
                    indexChange.setChangeType(IndexChange.ChangeType.UPDATE);
                    return indexChange;
                } else {//seeting 一样
                    indexChange.setChangeType(IndexChange.ChangeType.NO_CHANGE);
                }
            } else {//需要添加Setting
                indexChange.setChangeType(IndexChange.ChangeType.SETTING_ADD);
                changes.put(ESKeyWord.Setting.settings.name(), settings);
            }

            //比较mapping
            Map<String, Object> mapping = compareMapping((Map<String,Object>)oldIndex.get(ESKeyWord.Mapping.mapping.name()), newIndex.get(ESKeyWord.Mapping.mapping.name()));
            Object mappingType = mapping.get(ESKeyWord.Mapping.mapping.name());
            if (null != mappingType) {//查看是否有索引未变
                if (IndexChange.ChangeType.UPDATE == mappingType) {//需要更新配置
                    indexChange.setChangeType(IndexChange.ChangeType.UPDATE);
                    return indexChange;
                }
            } else {//需要添加mapping
                if(indexChange.getChangeType() == IndexChange.ChangeType.SETTING_ADD){//如果setting也需要添加配置
                    indexChange.setChangeType(IndexChange.ChangeType.ADD);
                }else{
                    indexChange.setChangeType(IndexChange.ChangeType.MAPPING_ADD);//只需要添加mapping
                }
                changes.put(ESKeyWord.Mapping.mapping.name(), mapping);
            }
            indexChange.setIndexChange(changes);
        }
        return indexChange;
    }

    /**
     * 使用redis 缓存 mapping ，key 默认使用索引名
     *
     * @param map
     * @throws MappingException
     */
    @Override
    public void cacheMap(MappingMap map) throws Exception {
        redis.setObject(map.getIndex(), map.getIndexMapping());
    }

    /**
     * 通过redis 获取缓存的映射
     *
     * @param index
     * @return
     * @throws Exception
     */
    @Override
    public Map<String,Object> getMappingByCache(String index) throws Exception {
        return (Map<String,Object>)redis.getObjectOfSort(index, LinkedHashMap.class);
    }

    /**
     * 使用MD5 加密 对象
     *
     * @param obj
     * @return
     */
    public String encryptObject(Object obj) {
        return MD5.md5(JSON.toJSONString(obj));
    }

    /**
     * 比较mapping 如果新索引存在新的列就添加并返回，如果key 相等而映射不等则返回update 状态
     *
     * @param oldMapping
     * @param newMapping
     * @return
     */
    protected Map<String, Object> compareMapping(Map<String, Object> oldMapping, Map<String, Object> newMapping) {

        Map<String, Object> resultMap = new HashMap<>();
        if (encryptObject(oldMapping).equals(encryptObject(newMapping))) {
            resultMap.put(ESKeyWord.Mapping.mapping.name(), IndexChange.ChangeType.NO_CHANGE);
            return resultMap;
        } else {
            for (Map.Entry<String, Object> newField : newMapping.entrySet()) {
                for (Map.Entry<String, Object> oldField : oldMapping.entrySet()) {
                    if (newField.getKey().equals(oldField.getKey())) {//如果属性key相等
                        if (encryptObject(newField.getValue()).equals(encryptObject(oldField.getValue()))) {//如果映射相等
                            resultMap.put(newField.getKey(), IndexChange.ChangeType.NO_CHANGE);
                        } else {
                            resultMap.put(ESKeyWord.Mapping.mapping.name(), IndexChange.ChangeType.UPDATE);//说明映射已经改变了，需要线下处理
                            return resultMap;
                        }
                        break;
                    }
                }
                if (resultMap.get(newField.getKey()) == null) {//说明未在旧映射中找到，表明这个是新添加的
                    resultMap.put(newField.getKey(), IndexChange.ChangeType.MAPPING_ADD);
                }
            }
            Map<String, Object> needAdd = new HashMap<>();//需要添加的field
            for (Map.Entry<String, Object> field : resultMap.entrySet()) {
                if (field.getValue().equals(IndexChange.ChangeType.MAPPING_ADD)) {
                    needAdd.put(field.getKey(), newMapping.get(field.getKey()));
                }
            }
            if(needAdd.keySet().size() == 0){//说明减少了字段，默认为未改变，不用处理
                resultMap.put(ESKeyWord.Mapping.mapping.name(), IndexChange.ChangeType.NO_CHANGE);
                return resultMap;
            }
            return needAdd;
        }

    }

    /**
     * 比较配置是否相等
     *
     * @param oldSettings 老的settings配置
     * @param newSettings 新的settings器配置
     * @return
     */
    protected Map<String, Object> compareAnalysis(Map<String, Object> oldSettings, Map<String, Object> newSettings) {

        Map<String, Object> setting = new HashMap<>();

        if (encryptObject(oldSettings).equals(encryptObject(newSettings))) {
            setting.put(ESKeyWord.Setting.settings.name(), IndexChange.ChangeType.NO_CHANGE);
            return setting;
        }
        //比较分词器
        Map<String, Object> oldAnalyzer = getAnalyzer(oldSettings);//旧的分词器
        Map<String, Object> newAnalyzer = getAnalyzer(newSettings);//新的分词器
        if (null == oldAnalyzer && newAnalyzer == null) {
            setting.put(ESKeyWord.Setting.settings.name(), IndexChange.ChangeType.NO_CHANGE);
            return setting;
        } else if (null == oldAnalyzer && newAnalyzer != null) {//需要添加的settings
            return newAnalyzer;
        }  else if(null != oldAnalyzer && newAnalyzer ==null){//如果oldAnalysis 不为空，newAnalysis 为空则认为未改变
            setting.put(ESKeyWord.Setting.settings.name(), IndexChange.ChangeType.NO_CHANGE);
            return setting;
        }else {
            if (encryptObject(oldAnalyzer).equals(encryptObject(newAnalyzer))) {
                setting.put(ESKeyWord.Setting.settings.name(), IndexChange.ChangeType.NO_CHANGE);
                return setting;
            } else {//对分析器进行逐一比较
                for (Map.Entry<String, Object> newAn : newAnalyzer.entrySet()) {
                    for (Map.Entry<String, Object> oldAN : oldAnalyzer.entrySet()) {
                        if (newAn.getKey().equals(oldAN.getKey())) {
                            if (encryptObject(newAn.getValue()).equals(encryptObject(oldAN.getValue()))) {//如果分词器相等
                                setting.put(oldAN.getKey(), IndexChange.ChangeType.NO_CHANGE);
                            } else {
                                setting.put(ESKeyWord.Mapping.mapping.name(), IndexChange.ChangeType.UPDATE);//说明分词器已经改变了，需要线下处理
                                return setting;
                            }
                            break;
                        }
                    }
                    if (setting.get(newAn.getKey()) == null) {//说明未在旧的分词器中找到新的分词器说明需要添加
                        setting.put(newAn.getKey(), IndexChange.ChangeType.SETTING_ADD);
                    }
                }
                Map<String, Object> needAdd = new HashMap<>();//需要添加的分词器
                for (Map.Entry<String, Object> field : setting.entrySet()) {//筛选出需要添加的分词器
                    if (field.getValue().equals(IndexChange.ChangeType.SETTING_ADD)) {
                        needAdd.put(field.getKey(), newAnalyzer.get(field.getKey()));
                    }
                }

                if(needAdd.keySet().size() == 0){//说明减少了配置，默认为未改变
                    setting.put(ESKeyWord.Setting.settings.name(), IndexChange.ChangeType.NO_CHANGE);
                    return setting;
                }
                Map<String, Object> analysis = new TreeMap<>();
                Map<String, Object> analyzer = new TreeMap<>();
                analyzer.put(ESKeyWord.Setting.analyzer.name(), needAdd);
                analysis.put(ESKeyWord.Setting.analysis.name(), analyzer);
                return analysis;

            }

        }
    }

    /**
     * 比较分片配置 是否发生改变
     * @param oldSettings 旧的配置
     * @param newSettings 新的配置
     * @return
     */
    protected Boolean compareShards(Map<String, Object> oldSettings, Map<String, Object> newSettings){

        String shardsKey=ESKeyWord.Setting.number_of_shards.name();
        String replicaseKey =ESKeyWord.Setting.number_of_replicas.name();

        if(oldSettings.get(shardsKey).equals(newSettings.get(shardsKey)) && oldSettings.get(replicaseKey).equals(newSettings.get(replicaseKey))){
            return  Boolean.FALSE;
        }else{
            return Boolean.TRUE;
        }

    }

    /**
     * 获取分析器里面的所有的分词器
     *
     * @param analysis
     * @return
     */
    protected Map<String, Object> getAnalyzer(Map<String, Object> analysis) {
        Object obj = analysis.get(ESKeyWord.Setting.analysis.name());
        if(null != obj){
            if (obj instanceof Map) {
                Map<String, Object> map = (Map<String, Object>) obj;
                Object analyzer = map.get(ESKeyWord.Setting.analyzer.name());
                if (analyzer instanceof Map) {
                    return (Map<String, Object>) analyzer;
                }
            }
            throw new MappingException(analysis+",{},mapping 解析异常");
        }
        return null;
    }
}
