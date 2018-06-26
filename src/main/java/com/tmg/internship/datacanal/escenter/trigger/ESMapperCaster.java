package com.tmg.internship.datacanal.escenter.trigger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.parser.Feature;
import com.tmg.commons.utils.SpringUtils;
import com.tmg.internship.datacanal.escenter.esengine.service.HighLevelClientService;
import com.tmg.internship.datacanal.escenter.exception.ESTriggerException;
import com.tmg.internship.datacanal.escenter.executer.AbstractExecuter;
import com.tmg.internship.datacanal.escenter.executer.BaseESAction;
import com.tmg.internship.datacanal.escenter.moduls.config.Tokenizer;
import com.tmg.internship.datacanal.escenter.moduls.config.event.EventHandle;
import com.tmg.internship.datacanal.escenter.moduls.index.MappingMap;
import com.tmg.internship.datacanal.escenter.moduls.mapping.ESKeyWord;
import com.tmg.internship.datacanal.escenter.moduls.mapping.cast.BasicAbstractCaster;
import com.tmg.internship.datacanal.escenter.moduls.mapping.cast.BasicMappingCaster;
import com.tmg.internship.datacanal.escenter.moduls.mapping.cast.MappingType;
import com.tmg.internship.datacanal.escenter.trigger.function.FunctionStrategy;
import com.tmg.utils.redis.SimpleRedisTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;


/**
 * ES mapper 构造器
 *
 * @author xiangjing
 * @date 2018/6/6
 * @company 天极云智
 */
public class ESMapperCaster {

    public static Logger logger = LoggerFactory.getLogger(ESMapperCaster.class);

    /**
     * 默认的文档类型
     */
    private static final String default_doc = MappingMap.default_doc;

    //数据库名和表名连接符
    public static final String default_connect = AbstractExecuter.default_connect;
    //别名连接符
    public static final String default_aliase = BasicAbstractCaster.default_aliase;

    //column 对应field
    private static Map<String, String> colRelationField = null;


    /**
     * 索引版本
     */
    private static final String index_version = BaseESAction.index_version;

    /**
     * redis 工具类
     */
    private static SimpleRedisTool simpleRedisTool = new SimpleRedisTool();

    /**
     * es 引擎
     */
    private static HighLevelClientService clientService;

    static {
        clientService = SpringUtils.getContext().getBean(HighLevelClientService.class);
    }

    /**
     * 通过ES来解析索引的配置与映射
     *
     * @param handle inset 的配置
     * @param map       不同索引下的不同列的数据
     * @param schemaName mapping 节点的名称
     * @param relations column-映射的field
     * @return
     */
    public static Map<String, Map<String,Object>> parseIndex(EventHandle handle ,Map<String, Map<String, Object>> map,String schemaName,Map<String,String> relations) {
        if (null != map && !map.isEmpty()) {
            colRelationField = relations;
            Map<String, Map<String,Object>> index = new TreeMap<>();
            Map<String, Object> mapping = parseIndexMapping(map);//解析mapping
            index.put(ESKeyWord.aliases.aliases.name(), new TreeMap<String,Object>(){{
                put(getIndexAliases(schemaName,handle.getIndexName()),"{}");
            }});//别名
            index.put(ESKeyWord.Mapping.mapping.name(),mapping);//映射
            Map<String,Object> setting = parseIndexSetting(mapping,handle);
            if(null != setting){
                index.put(ESKeyWord.Setting.settings.name(), setting);//ES 索引配置
            }

            return index;
        }
        return null;
    }

    /**
     * 解析配置
     *
     * @param mapping mappings映射配置
     * @param handle insert 的配置文件
     * @return
     */
    public static Map<String, Object> parseIndexSetting(Map<String, Object> mapping,EventHandle handle) {
        Map<String,Object> settings =new TreeMap<>();
        if (null != mapping && !mapping.isEmpty()) {
            JSONObject jsonObject = new JSONObject(mapping);
            String index = jsonObject.toJSONString();
            if(index.contains(Tokenizer.PATH.getCode())){//通过整体判断mapping里面是否存在path_hierarchy分词器
                Map<String, Object> analyzer = createAnalyzer(Tokenizer.PATH);
                if (null != analyzer) {
                    settings.put(ESKeyWord.Setting.analysis.name(), analyzer);

                }
            }
        }
        //设置主分片数
        settings.put(ESKeyWord.Setting.number_of_shards.name(),handle.getNumberOfShards());
        //设置副分片数
        settings.put(ESKeyWord.Setting.number_of_replicas.name(),handle.getNumberOfReplicas());

        return settings;

    }

    /**
     * 解析映射 只有列级别节点的映射，无doc 和properties 节点
     *
     * @param map
     * @return
     */
    public static Map<String, Object> parseIndexMapping(Map<String, Map<String, Object>> map) {
        Map<String, Object> mapping = new TreeMap<String, Object>();

        Map<String, Object> temp = null;
        String indexName = null;
        for (Map.Entry<String, Map<String, Object>> entry : map.entrySet()) {
            indexName = entry.getKey();
            indexName = getIndexNameByVersion(indexName);//获取当前版本的索引名称
            temp = getFieldMapping(indexName, entry.getValue().keySet());
            if (null != temp) {
                mapping.putAll(temp);
            }
        }

        if (!mapping.isEmpty()) {
            return mapping;
        }
        return null;
    }


    /**
     * @param indexName 索引名
     * @param fields    需要添加field的名称
     * @return
     */
    public static Map<String, Object> getFieldMapping(String indexName, Set<String> fields) {

        if (null != fields && fields.size() != 0) {
            Map<String, Object> allFieldMapping = getESMapping(indexName);
            Object mapping = null;
            MappingType type = null;
            Map<String, Object> fieldMapping = new TreeMap<>();
            for (String field : fields) {
                type = FunctionStrategy.getMappintType(field);
                if(null != type){//判断是不是函数类型
                    Map<String,Object> map = new TreeMap();
                    map.put(ESKeyWord.Mapping.type.name(),type.getCode());
                    fieldMapping.put(colRelationField.get(indexName+"."+field), map);

                }else{
                    mapping = allFieldMapping.get(field);
                    if (null != mapping) {
                        fieldMapping.put(colRelationField.get(indexName+"."+field), mapping);
                    }else{
                        logger.warn(indexName+" index not have "+field +" column!");
                    }
                }

            }
            return fieldMapping;
        } else {
            return null;
        }
    }

    /**
     * 获取ES 索引里面mapping 的配置 直到 properties下面的节点
     *
     * @param indexName
     * @return
     */
    public static Map<String, Object> getESMapping(String indexName) {
        Map<String, Object> mapper = getESIndexMapping(indexName);
        return getESMappingProperties(indexName, mapper);
    }

    /**
     * 处理ES返回mapping
     *
     * @param indexName 索引名称
     * @param mapper    从ES 中查询出的mapping配置
     * @return
     */
    private static Map<String, Object> getESMappingProperties(String indexName, Map<String, Object> mapper) {
        try {
            Map<String, Object> index = JSONObject.parseObject(mapper.get(indexName).toString());
            Map<String, Object> mappings = JSONObject.parseObject(index.get(ESKeyWord.Mapping.mappings.name()).toString());
            Map<String, Object> document = JSONObject.parseObject(mappings.get(default_doc).toString());
            String mappers = ((JSONObject)document.get(ESKeyWord.Mapping.properties.name())).toJSONString();
            return JSON.parseObject(mappers, new TypeReference<Map>() {
            },Feature.OrderedField);
        } catch (Exception e) {
            throw new ESTriggerException(indexName + " index not exists or not available,please Administrators to look at the index as possible", e);
        }
    }

    /**
     * 查询IndexName 索引里面的映射
     *
     * @param indexName
     * @return
     */
    private static Map<String, Object> getESIndexMapping(String indexName) {
        try {
            return clientService.getIndexMapping(indexName, true);
        } catch (Exception e) {
            throw new ESTriggerException(indexName + " index not exists or not available,please Administrators to look at the index as possible", e);
        }
    }

    /**
     * 根据版本号获取索引名称
     *
     * @param indexName 索引名称
     * @return
     */
    private static String getIndexNameByVersion(String indexName) {
        try {
            Integer version = simpleRedisTool.getObject(indexName + index_version, Integer.class);
            if (null != version) {
                return indexName + "_" + version;
            }
            return indexName;
        } catch (UnsupportedEncodingException e) {
            logger.warn(indexName + "index get version from redis faild");
            return indexName;
        }
    }

    /**
     * 获取索引别名
     *判断索引名称是不是完整的索引名称，如果是则返回，如果不是则拼接
     * @param schemaName mapping 节点名称
     * @param mergeName insert 节点下面的indexName 跟collectionNode 节点的name 类似
     * @return
     */
    private static String getIndexAliases(String schemaName,String mergeName) {
        if(mergeName.contains(default_connect)){//判断是不是完整的索引名
            return mergeName.replace(default_connect,default_aliase);
        }else{
            return schemaName+default_aliase+mergeName;
        }
    }

    /**
     * 创建自定义分词器
     *
     * @param tokenizer 要创建的分词器类型
     * @return
     */
    private static Map<String, Object> createAnalyzer(Tokenizer tokenizer) {
        BasicMappingCaster caster = new BasicMappingCaster();
        return caster.createAnalyzer(tokenizer);
    }


}
