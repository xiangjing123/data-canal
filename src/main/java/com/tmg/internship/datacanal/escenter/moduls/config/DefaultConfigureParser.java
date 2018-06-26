package com.tmg.internship.datacanal.escenter.moduls.config;

import com.tmg.internship.datacanal.escenter.exception.ESConfigParseException;
import com.tmg.internship.datacanal.escenter.exception.ESException;
import com.tmg.internship.datacanal.escenter.moduls.config.event.*;
import com.tmg.internship.datacanal.escenter.parser.Event;
import com.tmg.utils.StringUtils;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 默认的配置文件解析器
 *
 * @author xiangjing
 * @date 2018/5/8
 * @company 天极云智
 */
public class DefaultConfigureParser extends AbstractConfigureParser {

    public static final Logger logger = LoggerFactory.getLogger(DefaultConfigureParser.class);

    /**
     * 定义操作类型，有3中操作类型
     */
    public static final String[] handleType = {"delete", "update", "insert"};


    public DefaultConfigureParser() {
    }

    /**
     * 设置配置文件加载路径
     *
     * @param path
     */
    public DefaultConfigureParser(String path) {
        this.path = path;
    }

    /**
     * 根据数据集collection 获取field 属性配置
     *
     * @param collectionNode 数据集 collection 的 节点 文本
     * @return
     * @throws Exception
     */
    @Override
    public List<FieldNode> parseMapping(Element collectionNode) throws Exception {
        Iterator fields = collectionNode.elementIterator("field");

        List<FieldNode> array = new ArrayList<>();

        FieldNode fieldNode = null;

        Element filed = null;
        while (fields.hasNext()) {
            filed = (Element) fields.next();
            fieldNode = new FieldNode();

            String columnName = filed.elementTextTrim("columnName");
            String fieldName = filed.elementTextTrim("fieldName");
            MappingRule rule = parseMappingRule(filed);

            /**
             * 先验证columnName 和fieldName 是否合理
             */
            if (StringUtils.isTrimEmpty(fieldName) && StringUtils.isTrimEmpty(columnName)) {
                throw new ESConfigParseException(collectionNode.elementText("name") + "数据集下面的 field 有fieldName 和columnName 为空的节点");
            }
            if (StringUtils.isTrimEmpty(columnName) && !StringUtils.isTrimEmpty(fieldName)) {
                if (null == rule) {
                    throw new ESConfigParseException(collectionNode.elementText("name") + "数据集下面的 fieldName为" + fieldName + " 的field节点未设置映射规则");
                } else {
                    if ((null == rule.getConcat() || rule.getConcat().size() == 0) && (null == rule.getReplaceAll() || rule.getReplaceAll().size() == 0)) {
                        throw new ESConfigParseException(collectionNode.elementText("name") + "数据集下面的 fieldName为" + fieldName + " 的field节点未设置映射规则");
                    }
                }
            }

            fieldNode.setColumnName(columnName);
            fieldNode.setFiledName(fieldName);
            fieldNode.setMappingRule(rule);//设置映射规则
            fieldNode.setMappingType(checkMappingType(filed.elementTextTrim("mappingType")));//校验数据类型
            fieldNode.setTokenizer(checkTokenizer(filed.elementTextTrim("tokenizer")));//校验分词器
            fieldNode.setKeyword(checkKeyword(filed.elementTextTrim("keyword")));//校验keyword
            fieldNode.setFieldData(Boolean.parseBoolean(filed.elementTextTrim("fieldData")));//设置fieldData
            fieldNode.setFormat(checkDateFormat(filed.elementTextTrim("format")));//校验时间格式是否正确
            fieldNode.setSubFieldList(parseSubField(filed));//设置子域 属性
            fieldNode.setPropertyNodeList(parsePropertyNode(filed));//设置对象属性配置，针对nested和object

            array.add(fieldNode);

        }
        checkFieldName(array, collectionNode.elementTextTrim("name"));//校验fieldName 是否存在相同

        return array;
    }

    /**
     * 解析数据集节点的分片设置
     *
     * @param handle onInsert 节点下面的insert 节点
     * @param eventHandle           collection 节点对象
     */
    public void parseMergeShardsSetting(Element handle, EventHandle eventHandle) {
        Integer numberOfShards = getShardsSetting(handle, "numberOfShards");
        Integer numberOfReplicas = getShardsSetting(handle, "numberOfReplicas");

        //设置主分片
        if (null == numberOfShards) {
            eventHandle.setNumberOfShards(configure.getNumberOfShards());
        } else {
            eventHandle.setNumberOfShards(numberOfShards);

        }
        //设置副分片
        if (null == numberOfReplicas) {
            eventHandle.setNumberOfReplicas(configure.getNumberOfReplicas());
        } else {
            eventHandle.setNumberOfReplicas(numberOfReplicas);
        }
    }

    /**
     * 根据数据集collection 解析触发trigger 节点的配置
     *
     * @param collectionNode 数据集 collection 的 节点 文本
     * @return
     * @throws Exception
     */
    @Override
    public List<Trigger> parseEvent(Element collectionNode) throws Exception {

        Element triggers = collectionNode.element("triggers");//获取trigger 节点

        if (null == triggers) {
            return null;
        }

        Iterator events = triggers.elementIterator();
        List<Trigger> array = new ArrayList<>();

        Trigger trigger = null;

        Element eventNode = null;
        Event event = null;
        while (events.hasNext()) {
            eventNode = (Element) events.next();//获取onUpdate，onInsert，onDelete 节点
            trigger = new Trigger();
            Event eventType = getEvent(eventNode.getName());
            trigger.setEvent(eventType);
            if(eventType == Event.INSERT){
                trigger.setFieldList(parseMergeEventFiled(eventNode));
            }else{
                trigger.setFieldList(parseEventFiled(eventNode));
            }
            array.add(trigger);
        }
        return array;
    }

    /**
     * 校验 field节点 fieldName 是否存在同名
     *
     * @param fieldNodes
     * @param parentName
     * @return
     */
    protected void checkFieldName(List<FieldNode> fieldNodes, String parentName) {

        Set<String> names = new HashSet<>();

        fieldNodes.forEach(field -> names.add(field.getFiledName()));

        if (names.size() < fieldNodes.size()) {
            throw new ESConfigParseException(this.path + ",{},配置文件 " + parentName + "数据域下面的 field 节点中 fieldName 重复");
        }

    }


    /**
     * 解析 fieldNode 下面的子域属性的值
     *
     * @param field
     * @return
     * @throws Exception
     */
    protected List<SubField> parseSubField(Element field) throws Exception {
        Iterator subFields = field.elementIterator("subField");

        List<SubField> array = new ArrayList<>();

        SubField subFieldNode = null;

        Element subField = null;
        while (subFields.hasNext()) {
            subField = (Element) subFields.next();
            subFieldNode = new SubField();

            String filedName = subField.elementTextTrim("fieldName");

            if (StringUtils.isTrimEmpty(filedName)) {
                throw new ESConfigParseException(field.getParent().elementText("name") + "数据集下面的" + field.elementText("columnName") + "filed下面的subField 节点的 fieldName 属性未配置");
            }

            Tokenizer tokenizer = checkTokenizer(subField.elementTextTrim("tokenizer"));
            if (null == tokenizer) {
                throw new ESConfigParseException(field.getParent().elementText("name") + "数据集下面的 " + field.elementText("columnName") + "属性下面的subField 节点的 tokenizer 属性未配置");
            }
            subFieldNode.setFiledName(filedName);
            subFieldNode.setTokenizer(tokenizer);
            subFieldNode.setMappingType(checkMappingType(subField.elementTextTrim("mappingType")));
            array.add(subFieldNode);
        }

        checkSubFieldName(array, field.elementText("fieldName"));//校验子域节点是否存在filedName 冲突
        return array;
    }

    /**
     * 解析 fieldNode 下面的子属性的值
     *
     * @param field
     * @return
     * @throws Exception
     */
    protected List<PropertyNode> parsePropertyNode(Element field) throws Exception {
        Element element = field.element("properties");

        if (null == element) {
            return null;
        }
        Iterator properties = element.elementIterator("property");

        List<PropertyNode> array = new ArrayList<>();

        PropertyNode propertyNode = null;

        Element property = null;
        while (properties.hasNext()) {
            property = (Element) properties.next();
            propertyNode = new PropertyNode();

            String columnName = property.elementTextTrim("columnName");

            if (StringUtils.isTrimEmpty(columnName)) {
                throw new ESConfigParseException(field.getParent().elementText("name") + "数据集下面的" + field.elementText("columnName") + "节点下面的properties节点下的 columnName 属性未配置");
            }
            propertyNode.setColumnName(columnName);
            propertyNode.setTokenizer(checkTokenizer(property.elementTextTrim("tokenizer")));
            propertyNode.setMappingType(checkMappingType(property.elementTextTrim("mappingType")));
            propertyNode.setKeyword(checkKeyword(property.elementTextTrim("keyword")));
            propertyNode.setFormat(checkDateFormat(property.elementTextTrim("format")));
            array.add(propertyNode);
        }

        if (array.size() != 0) {//校验子属性配置columnName 是否重复
            Set<String> names = new HashSet<>();
            array.forEach(node -> names.add(node.getColumnName()));

            if (names.size() < array.size()) {
                throw new ESConfigParseException(this.path + ",{},配置文件:" + field.getParent().elementText("name") + "数据集下面的" + field.elementText("columnName") + "属性节点下的 properties 节点中 columnName 重复");
            }
        }

        return array;
    }

    /**
     * 校验 subField 节点 fieldName 是否存在同名
     *
     * @param subFields 子域配置
     * @param fieldName 父节点
     * @return
     */
    protected void checkSubFieldName(List<SubField> subFields, String fieldName) {

        Set<String> names = new HashSet<>();

        subFields.forEach(field -> names.add(field.getFiledName()));

        if (names.size() < subFields.size()) {
            throw new ESConfigParseException(this.path + ",{},配置文件" + fieldName + "属性节点下的 subField 节点中 fieldName 重复");
        }

    }

    /**
     * 解析配置文件trigger 下的 field 节点的配置
     *
     * @param eventNode
     * @return
     * @throws Exception
     */
    protected List<EventField> parseEventFiled(Element eventNode) throws Exception {

        Iterator elementIterator = eventNode.elementIterator();//获取trigger 事件节点下面的filed 节点

        List<EventField> array = new ArrayList<>();

        EventField eventField = null;

        Element field = null;

        String nodeName = null;

        while (elementIterator.hasNext()) {
            field = (Element) elementIterator.next();//获取onUpdate，onInsert，onDelete 节点
            nodeName = field.getName();
            if (!"field".equals(nodeName)) {//判断是不是Field 节点
                throw new ESConfigParseException("trigger 节点中不识别的 子节点：" + nodeName);
            }
            eventField = new EventField();

            eventField.setColumnName(field.elementTextTrim("columnName"));
            eventField.setWhere(field.elementTextTrim("where"));//获取where 条件
            eventField.setHandleList(parseEventHandle(field));
            array.add(eventField);
        }
        return array;
    }

    /**
     * 解析OnInsert配置文件下的 field 节点的配置
     *
     * @param eventNode (OnInsert 节点的element)
     * @return
     * @throws Exception
     */
    protected List<EventField> parseMergeEventFiled(Element eventNode) throws Exception {
        Element field = eventNode.element("field");//获取trigger 事件节点下面的filed 节点

        if(null != field){
            List<EventField> array = new ArrayList<>();
            EventField  eventField = new EventField();
            eventField.setHandleList(parseMergeHandle(field));
            array.add(eventField);
            return array;
        }else {
            return null;
        }

    }

    /**
     * 解析filed 所有操作事件的配置
     *
     * @param eventField
     * @return
     * @throws Exception
     */
    protected List<EventHandle> parseEventHandle(Element eventField) throws Exception {

        List<EventHandle> eventHandles = new ArrayList<>();
        for (String type : this.handleType) {
            eventHandles.addAll(parseEventHandle(eventField, type));
        }
        return eventHandles;
    }

    /**
     * 解析配置文件trigger 下的 field 节点中的 update/insert/delete 单一节点的配置
     *
     * @param eventField filed 节点
     * @return
     * @throws ESException
     */
    protected List<EventHandle> parseEventHandle(Element eventField, String nodeName) throws Exception {

        Iterator elementIterator = eventField.elementIterator(nodeName);//获取trigger事件 filed 节点下面的update 节点

        List<EventHandle> array = new ArrayList<>();

        EventHandle eventHandle = null;

        Element element = null;

        while (elementIterator.hasNext()) {
            element = (Element) elementIterator.next();//获取update，insert，delete 节点
            eventHandle = new EventHandle();
            eventHandle.setEvent(checkEvent(nodeName));
            eventHandle.setFiled(element.elementTextTrim("field"));//获取要设置的es 索引里面的索引
            eventHandle.setValue(element.elementTextTrim("value"));//获取要设置的值
            eventHandle.setConditionList(parseEventCondition(element));
            array.add(eventHandle);
        }
        return array;
    }

    /**
     * 解析合并下面的
     * @param eventField
     * @return
     * @throws Exception
     */
    protected List<EventHandle> parseMergeHandle(Element eventField) throws Exception {

        String nodeName = Event.INSERT.name().toLowerCase();
        Iterator elementIterator = eventField.elementIterator(nodeName);//OnInsert field 节点下面的insert配置

        List<EventHandle> array = new ArrayList<>();

        EventHandle eventHandle = null;

        Element element = null;

        while (elementIterator.hasNext()) {
            element = (Element) elementIterator.next();//获取onUpdate，onInsert，onDelete 节点
            eventHandle = new EventHandle();
            parseMergeShardsSetting(element,eventHandle);//设置其分片配置
            eventHandle.setEvent(checkEvent(nodeName));//设置操作事件这里默认是INSERT 事件
            eventHandle.setIndexName(element.elementTextTrim("indexName"));
            eventHandle.setSourceNodeList(parseEventSourceNode(element.elementIterator("source")));

            array.add(eventHandle);
        }
        return array;
    }


    /**
     * 解析 insert 合并下面的source 节点
     * @param sourceFieldList insert 下面的source 节点
     * @return
     * @throws Exception
     */
    protected List<SourceNode> parseEventSourceNode(Iterator sourceFieldList) throws Exception {

        List<SourceNode> sources = new ArrayList<>();
        Element source = null;
        if(null != sourceFieldList){
            while (sourceFieldList.hasNext()){
                source =(Element)sourceFieldList.next();
                sources.add(parseEventSourceNode(source));
            }
        }
        if(sources.size() == 0){
            throw  new ESConfigParseException("onInsert 节点下面的 source 配置不能为空");
        }

        return sources;
    }

    /**
     * 解析 insert 合并下面的source 节点
     * @param sourceField insert 下面的source 节点
     * @return
     * @throws Exception
     */
    protected SourceNode parseEventSourceNode(Element sourceField) throws Exception {

        SourceNode source = new SourceNode();

        source.setIndexName(sourceField.elementTextTrim("indexName"));//解析source 节点下面的indexName 可以为空
        String exportFields = sourceField.elementTextTrim("exportFields");
        if(StringUtils.isEmpty(exportFields)){
            throw  new ESConfigParseException("配置文件解析错误:onInsert 触发下面的 source 节点的 exportFields 不能为空");
        }
        try {
            source.setExportFields(exportFields);//需要用于合并的属性
        }catch (Exception e){
            throw  new ESConfigParseException("配置文件解析错误:onInsert 触发下面的 source 节点的 exportFields 解析错误",e);
        }

        source.setConditionList(parseEventCondition(sourceField));//查询条件
        source.setAllowNull(Boolean.parseBoolean(sourceField.elementTextTrim("allowNull")));//是否允许为空

        if(!StringUtils.isEmpty(source.getIndexName()) && (source.getConditionList() == null || source.getConditionList().size() == 0)){
            throw  new ESConfigParseException("配置文件解析错误:onInsert 触发下面的 source 节点 indexName 存在, condition 不能为空");
        }

        return source;
    }

    /**
     * 解析配置文件trigger 下的 field 事件操作节点的配置
     *
     * @param eventHandleNode update 或者insert 或者delete节点
     * @return
     * @throws ESException
     */
    protected List<Condition> parseEventCondition(Element eventHandleNode) throws ESException {

        Element conditions = eventHandleNode.element("condition");//获取trigger 事件节点下面的filed 节点

        if (null == conditions) {
            return null;
        }

        Iterator iterator = conditions.elementIterator("eq");//获取condition 条件节点中的所有条件
        List<Condition> array = new ArrayList<>();

        Condition condition = null;
        Element element = null;

        while (iterator.hasNext()) {
            element = (Element) iterator.next();//获取onUpdate，onInsert，onDelete 节点

            condition =new Condition();

            condition.setColumnName(element.elementTextTrim("columnName"));//获取源数据列名
            condition.setFieldName(element.elementTextTrim("fieldName"));//获取ES 索引中的列名

            if(StringUtils.isEmpty(condition.getColumnName()) || StringUtils.isEmpty(condition.getFieldName())){//条件不能为空
                throw new ESConfigParseException("condition 节点中条件不能为空");
            }

            array.add(condition);

        }
        return array;
    }

    /**
     * 解析mappingRule
     *
     * @param field
     * @return
     */
    protected MappingRule parseMappingRule(Element field) {

        Element rule = field.element("mappingRule");
        if (null == rule) {
            return null;
        }

        List<MappingRule.Replace> array = new ArrayList<>();
        Iterator iterator = rule.elementIterator("replaceAll");

        Object obj = null;
        Element replaceALl;
        MappingRule.Replace replace = null;
        while (iterator.hasNext()) {
            obj = iterator.next();
            if (obj instanceof Element) {
                replaceALl = (Element) obj;

                replace = new MappingRule().new Replace();

                String regex = replaceALl.elementTextTrim("regex");
                String replacement = replaceALl.elementTextTrim("replacement");

                if (StringUtils.isTrimEmpty(regex) || StringUtils.isTrimEmpty(replacement)) {
                    continue;
                } else {
                    replace.setRegex(regex);
                    replace.setReplacement(replacement);
                    array.add(replace);
                }

            }
        }
        if (array.size() != 0) {
            MappingRule mappingRule = new MappingRule();
            mappingRule.setReplaceAll(array);
            mappingRule.setConcat(parseConcatRule(rule));
            return mappingRule;
        } else {
            return null;
        }
    }

    /**
     * 解析mappingRule 里面的concat
     *
     * @param mappingRule
     * @return
     */
    protected List<ConcatParam> parseConcatRule(Element mappingRule) {

        Element rule = mappingRule.element("concat");
        if (null == rule) {
            return null;
        }
        List<ConcatParam> array = new ArrayList<>();
        Iterator iterator = rule.elementIterator();
        Element concat;
        Object obj = null;
        ConcatParam param = null;
        while (iterator.hasNext()) {
            concat = (Element) iterator.next();
            switch (concat.getName()) {
                case "string":
                    array.add(new ConcatParam(ConcatParam.ParamType.COMMON, concat.getData()));
                    continue;
                case "fieldName":
                    array.add(new ConcatParam(ConcatParam.ParamType.FIELD, concat.getData()));
                    continue;
                case "columnName":
                    array.add(new ConcatParam(ConcatParam.ParamType.COLUMN, concat.getData()));
                    continue;
            }
        }
        if (array.size() != 0) {
            return array;
        } else {
            return null;
        }
    }

    /**
     * 将String 解析为boolean 类型
     *
     * @param bool
     * @return
     */
    @Deprecated
    protected Boolean parseBoolean(String bool) {
        if (StringUtils.isEmpty(bool)) {
            return null;
        } else {
            return bool.equalsIgnoreCase("true");
        }
    }

    /**
     * 通过 事件节点名称来判断 事件类型
     *
     * @param name
     * @return
     */
    protected static Event getEvent(String name) {

        if (StringUtils.isTrimEmpty(name)) {
            throw new ESConfigParseException("can't not read " + name + " 事件");
        }
        switch (name) {
            case "onInsert":
                return Event.INSERT;
            case "onUpdate":
                return Event.UPDATE;
            case "onDelete":
                return Event.DELETE;
            default:
                throw new ESConfigParseException("can't not read " + name + " 事件");
        }
    }
}
