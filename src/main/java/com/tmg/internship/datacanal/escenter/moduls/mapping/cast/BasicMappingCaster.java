package com.tmg.internship.datacanal.escenter.moduls.mapping.cast;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.tmg.internship.datacanal.escenter.common.DateIntervalArray;
import com.tmg.internship.datacanal.escenter.common.DateIntervalMap;
import com.tmg.internship.datacanal.escenter.common.ESMappingUtil;
import com.tmg.internship.datacanal.escenter.exception.MappingException;
import com.tmg.internship.datacanal.escenter.moduls.config.*;
import com.tmg.internship.datacanal.escenter.moduls.index.MappingMap;
import com.tmg.internship.datacanal.escenter.moduls.mapping.ESKeyWord;
import com.tmg.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Mapping 创造接口
 *
 * @author xiangjing
 * @date 2018/5/9
 * @company 天极云智
 */
public class BasicMappingCaster extends BasicAbstractCaster {

    public static final Logger logger = LoggerFactory.getLogger(BasicMappingCaster.class);

    //某个数据集下面的具体配置
    protected static FieldNode fieldNode = null;

    //field 下面的映射规则
    protected static Map<String, Object> map = null;

    public BasicMappingCaster() {
    }

    public BasicMappingCaster(ESConfigure configure,MappingNode mappingNode, CollectionNode collectionNode) {
        this.configure = configure;
        this.mappingNode = mappingNode;
        this.collectionNode = collectionNode;

    }

    /**
     * 获取field 节点 的配置
     *
     * @param fieldName
     */
    protected FieldNode getFieldNode(String fieldName) {
        if (this.collectionNode != null) {
            return this.collectionNode.getFieldNodeByField(fieldName);
        }
        return null;
    }


    /**
     * 创建映射
     *
     * @param map
     * @return
     * @throws MappingException
     */
    @Override
    public Map<String, Object> createIndexMaping(MappingMap map) throws MappingException {
        Map<String, Object> mappings = new TreeMap<>();
        Map<String, Object> fields = map.getFields();
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            mappings.put(entry.getKey(), parseFieldMapping(entry.getKey(), entry.getValue()));
        }

        return mappings;
    }

    /**
     * 创建索引配置
     *
     * @param map
     * @return
     * @throws MappingException
     */
    @Override
    public Map<String, Object> createIndexsettings(MappingMap map) throws MappingException {

        Map<String,Object> settings = new TreeMap<>();
        Map<String, Object> fields = map.getFields();
        Map<String, Object> analyzer = settingTokenizer(Tokenizer.PATH, fields);
        if (null != analyzer) {
            settings.put(ESKeyWord.Setting.analysis.name(), analyzer);
        }
        if(this.collectionNode !=null){
            //设置主分片数
            settings.put(ESKeyWord.Setting.number_of_shards.name(),collectionNode.getNumberOfShards());
            //设置副分片数
            settings.put(ESKeyWord.Setting.number_of_replicas.name(),collectionNode.getNumberOfReplicas());
        }else{
            //设置主分片数
            settings.put(ESKeyWord.Setting.number_of_shards.name(),configure.getNumberOfShards());
            //设置副分片数
            settings.put(ESKeyWord.Setting.number_of_replicas.name(),configure.getNumberOfReplicas());
        }
        return settings;
    }

    /**
     * 解析fieldName 返回相应的mapping
     *
     * @param fieldName  fieldName field 属性
     * @param fieldValue fieldValue 值
     * @return
     */
    protected Map<String, Object> parseFieldMapping(String fieldName, Object fieldValue) {
        map = new TreeMap<>();
        fieldNode = getFieldNode(fieldName);
        Map<String, Object> field = new TreeMap<>();
        String type = parseFieldType(fieldNode, fieldName, fieldValue);//解析出的es 映射类型
        field.put(ESKeyWord.Mapping.type.name(), type);//设置映射类型
        if (type.equals(MappingType.TEXT.getCode())) {//只有text类型的数据才能添加分词
            field.put(ESKeyWord.Setting.analyzer.name(), parseFieldTokenizer(fieldNode, fieldName).getCode());
            boolean keyword = parseFieldKeyword(fieldNode);//是否添加doc-values 文档
            Map<String, Object> subFields = new TreeMap<>();
            if (keyword) {//添加doc-values 设置
                subFields.put(ESKeyWord.Mapping.keyword.name(), addKeyword());
            }
            if (fieldNode != null && fieldNode.getSubFieldList() != null && fieldNode.getSubFieldList().size() != 0) {
                for (SubField subField : fieldNode.getSubFieldList()) {
                    subFields.put(subField.getFiledName(), addSubfield(subField));
                }
            }
            if(!subFields.isEmpty()){
                field.put(ESKeyWord.Mapping.fields.name(), subFields);
            }

            if(parseFieldData(fieldNode)){//是否设置filedDate
                field.put(ESKeyWord.Mapping.term_vector.name(),ESKeyWord.TermVector.with_positions_offsets.name());
                field.put(ESKeyWord.Mapping.fielddata.name(),true);
            }
        } else if (type.equals(MappingType.DATE.getCode())) {//如果是时间类型的数据需要设置时间格式
            if (null != fieldNode && !StringUtils.isEmpty(fieldNode.getFormat())) {//如果配置不为空
                field.put(ESKeyWord.Mapping.format.name(), fieldNode.getFormat());
            } else {
                String format = ESMappingUtil.getDateFormat();
                if (!StringUtils.isEmpty(format)) {
                    field.put(ESKeyWord.Mapping.format.name(), format);
                }
            }
        } else if (type.equalsIgnoreCase(MappingType.NESTED.getCode())) {//nested类型  需要解析其子属性
            Map<String, Object> properties = parseJSONField(fieldNode, fieldValue);
            if (null != properties) {
                field.put(ESKeyWord.Mapping.properties.name(), properties);
            }

        }
        return field;
    }

    /**
     * 此处不需要对fieldValue 进行验空，在index 数据处理模块已经严重
     *
     * @param fieldNode  节点的配置
     * @param fieldValue 属性值
     * @return 返回properties 节点下的属性配置
     */
    protected Map<String, Object> parseJSONField(FieldNode fieldNode, Object fieldValue) {
        if (null != fieldNode) {
            List<PropertyNode> propertyNodes = fieldNode.getPropertyNodeList();
            if (null != propertyNodes && propertyNodes.size() != 0) {//如果有相关配置则使用相关配置进行解析
                Map<String, Object> jsonFields = new TreeMap<>();
                if (fieldValue instanceof JSONObject) {//
                    JSONObject jsonObject = (JSONObject) fieldValue;
                    parseJSONField(jsonFields, propertyNodes, jsonObject);
                } else if (fieldValue instanceof JSONArray) {
                    JSONArray jsonArray = (JSONArray) fieldValue;
                    if (jsonArray.size() != 0) {
                        parseJSONField(jsonFields, propertyNodes, jsonArray.getJSONObject(0));//取一个jsonObject 进行解析
                    }
                }

                if (jsonFields.keySet().size() != 0) {
                    return jsonFields;
                }
            }
        }

        if(fieldValue instanceof DateIntervalMap){//如果是时间区间类型的则使用默认配置
            return parseDateIntervalMap((DateIntervalMap) fieldValue);
        }else if(fieldValue instanceof DateIntervalArray){
            return parseDateIntervalArray((DateIntervalArray) fieldValue);
        }

        return null;
    }

    /**
     * 解析时间区间类
     * @param map
     * @return
     */
    private Map<String,Object> parseDateIntervalMap(DateIntervalMap map){
        if(null != map && !map.isEmpty()){
            Map<String,Object> dates = new TreeMap<>();
            Map<String,Object> date = null;
            for(Map.Entry<String,Object> entry:map.entrySet()){
                date=new TreeMap<>();
                date.put(ESKeyWord.Mapping.type.name(), MappingType.DATE.getCode());
                date.put(ESKeyWord.Mapping.format.name(), ESMappingUtil.getDateFormat());
                dates.put(entry.getKey(),date);
            }
            return dates;
        }
        return null;
    }

    /**
     * 解析时间区间
     * @param array
     * @return
     */
    private Map<String,Object> parseDateIntervalArray(DateIntervalArray array){
        if(null != array && !array.isEmpty()){
            for(Object obj:array){
                if(obj instanceof DateIntervalMap){
                    return parseDateIntervalMap((DateIntervalMap)obj);
                }
            }
        }
        return null;
    }


    /**
     * @param propertyNodes 对象属性的配置的配置
     * @param jsonObject    json 对象
     * @return 返回子属性的相关映射
     */
    private void parseJSONField(Map<String, Object> jsonFields, List<PropertyNode> propertyNodes, JSONObject jsonObject) {
        Map<String, Object> result = null;
        for (PropertyNode node : propertyNodes) {
            result = parseJSONField(node, jsonObject);
            if (null != result) {
                jsonFields.putAll(result);
            }
        }
    }

    /**
     * @param propertyNode 对象属性的配置的配置
     * @param fieldValue   属性值
     * @return 返回子属性的相关映射
     */
    private Map<String, Object> parseJSONField(PropertyNode propertyNode, JSONObject fieldValue) {
        if (null != propertyNode) {
            Map<String, Object> map = new TreeMap<>();
            String columnName = propertyNode.getColumnName();
            //就算是子属性的值 如果为空，待后续处理
            String value = fieldValue.getString(columnName);
            String type = propertyNode.getMappingType();
            if (StringUtils.isEmpty(type)) {
                type = parserType(columnName, value);
            }
            map.put(ESKeyWord.Mapping.type.name(), type);//设置映射类型

            if (type.equals(MappingType.TEXT.getCode())) {//只有text类型的数据才能添加分词
                map.put(ESKeyWord.Setting.analyzer.name(), parsePropertyTokenizer(propertyNode, columnName).getCode());
                if (propertyNode.getKeyword()) {//只针对于text 类型
                    map.put(ESKeyWord.Mapping.fields.name(), getCompeleteKeyword());
                }
            } else if (type.equals(MappingType.DATE.getCode())) {//如果是时间类型的数据需要设置时间格式
                if (!StringUtils.isEmpty(propertyNode.getFormat())) {//如果配置不为空
                    map.put(ESKeyWord.Mapping.format.name(), propertyNode.getFormat());
                } else {
                    String format = ESMappingUtil.getDateFormat();
                    if (!StringUtils.isEmpty(format)) {
                        map.put(ESKeyWord.Mapping.format.name(), format);
                    }
                }
            }

            if (map.keySet().size() != 0){
                return new TreeMap<String,Object>(){{
                    put(columnName,map);
                }};
            }
        }
        return null;
    }


    /**
     * 解析数据类型
     *
     * @param fieldNode field 对应配置
     * @param key       ES对应属性 fieldName
     * @param value     fieldName 属性对应的值
     * @return
     */
    protected String parseFieldType(FieldNode fieldNode, String key, Object value) {
        String mappingType = null;
        if (null != fieldNode) {//如果无相关配置
            mappingType = fieldNode.getMappingType();
            if (!StringUtils.isEmpty(mappingType)) {//类型配置为空
                return mappingType;
            }
        }
        return parserType(key, value);
    }

    /**
     * 解析数据分词器
     *
     * @param fieldNode field 对应配置
     * @param key       ES对应属性 fieldName
     * @return
     */
    protected Tokenizer parseFieldTokenizer(FieldNode fieldNode, String key) {
        Tokenizer tokenizer = null;
        if (null != fieldNode) {//如果无相关配置
            tokenizer = fieldNode.getTokenizer();
            if (null != tokenizer) {//类型配置为空
                return tokenizer;
            }
        }
        return parseTokenier(key);
    }

    /**
     * 解析数据分词器
     *
     * @param propertyNode property 对应配置
     * @param key       ES对应属性 fieldName
     * @return
     */
    protected Tokenizer parsePropertyTokenizer(PropertyNode propertyNode, String key) {
        Tokenizer tokenizer = null;
        if (null != propertyNode) {//如果无相关配置
            tokenizer = propertyNode.getTokenizer();
            if (null != tokenizer) {//类型配置为空
                return tokenizer;
            }
        }
        return parseTokenier(key);
    }

    /**
     * 解析field keyword
     *
     * @param fieldNode
     * @return
     */
    protected boolean parseFieldKeyword(FieldNode fieldNode) {
        if (null != fieldNode) {//如果无相关配置
            return fieldNode.getKeyword();
        }
        return Boolean.TRUE;
    }

    /**
     * 解析field fieldData
     *
     * @param fieldNode
     * @return
     */
    protected boolean parseFieldData(FieldNode fieldNode) {
        if (null != fieldNode) {//如果无相关配置
            return fieldNode.getFieldData();
        }
        return Boolean.FALSE;
    }

    /**
     * 添加keyword 数据
     *
     * @return
     */
    protected Map<String, Object> getCompeleteKeyword() {
        return new TreeMap<String, Object>() {{
            put(ESKeyWord.Mapping.keyword.name(), addKeyword());
        }};
    }


    /**
     * 添加keyword 数据
     *
     * @return
     */
    protected Map<String, Object> addKeyword() {
        return new TreeMap<String, Object>() {{
            put(ESKeyWord.Mapping.type.name(), ESKeyWord.Mapping.keyword.name());
            put(ESKeyWord.Mapping.ignore_above.name(), 256);
        }};
    }

    /**
     * 属性子域属性
     *
     * @param subField 子域属性配置
     * @return
     */
    protected Map<String, Object> addSubfield(SubField subField) {
        return new TreeMap<String, Object>() {{
            put(ESKeyWord.Mapping.type.name(), MappingType.TEXT);
            put(ESKeyWord.Setting.analyzer.name(), subField.getTokenizer());
        }};
    }


    /**
     * 根据分词器设置配置文件
     *
     * @param tokenizer
     * @param fields
     * @return
     */
    protected Map<String, Object> settingTokenizer(Tokenizer tokenizer, Map<String, Object> fields) {

        for (String key : fields.keySet()) {
            if (tokenizer == parseFieldTokenizer(getFieldNode(key), key)) {//如果要创建的分词器，跟需要设置的分词器想等，则创建
                return createAnalyzer(tokenizer);
            }
        }
        return null;
    }

    /**
     * 创建自定义分词器
     *
     * @param tokenizer 要创建的分词器类型
     * @return
     */
    public Map<String, Object> createAnalyzer(Tokenizer tokenizer) {
        return new TreeMap<String, Object>() {{
            put(ESKeyWord.Setting.analyzer.name(), createTokenizer(tokenizer));
        }};
    }

    /**
     * 自定义tokenizer
     *
     * @param tokenizer
     * @return
     */
    protected Map<String, Object> createTokenizer(Tokenizer tokenizer) {
        return new TreeMap<String, Object>() {{
            put(tokenizer.getCode(), new TreeMap<String, Object>() {{
                put(ESKeyWord.Setting.tokenizer.name(), tokenizer.getCode());
            }});
        }};
    }
}
