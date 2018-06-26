package com.tmg.internship.datacanal.escenter.trigger;

import com.alibaba.fastjson.JSON;
import com.tmg.internship.datacanal.escenter.exception.ESException;
import com.tmg.internship.datacanal.escenter.exception.ESTriggerException;
import com.tmg.internship.datacanal.escenter.executer.BaseESAction;
import com.tmg.internship.datacanal.escenter.executer.ESAction;
import com.tmg.internship.datacanal.escenter.moduls.config.MappingNode;
import com.tmg.internship.datacanal.escenter.moduls.config.event.*;
import com.tmg.internship.datacanal.escenter.moduls.index.HandleMapping;
import com.tmg.internship.datacanal.escenter.moduls.index.MappingMap;
import com.tmg.internship.datacanal.escenter.moduls.mapping.ESKeyWord;
import com.tmg.internship.datacanal.escenter.moduls.mapping.cast.MappingType;
import com.tmg.internship.datacanal.escenter.parser.Event;
import com.tmg.internship.datacanal.escenter.trigger.function.FunctionStrategy;
import com.tmg.utils.StringUtils;

import java.util.*;

/**
 * 触发合并事件
 *
 * @author xiangjing
 * @date 2018/6/1
 * @company 天极云智
 */
public class MergeEventTrigger extends AbstractESTrigger {

    private static Trigger trigger = null;

    //触发数据集要插入的初始数据
    private static Map<String, Object> addData = null;

    //ES 主键
    private String id = null;

    //column 对应field
    private static Map<String, String> baseColRelationField = null;

    //数据集的对应的索引名称
    private String collectionIndexName = null;

    //column 对应field
    private Map<String, String> colRelationField = null;

    /**
     * 数据集对应的columns
     */
    private Map<String, Map<String, Object>> collectionColumns = null;

    /**
     * ES 直接操作对象
     */
    private ESAction action;


    /**
     * 单例模式
     */
    private void getSingleAction() {
        if (null == this.action) {
            this.action = new BaseESAction();
        }
    }

    @Override
    public void eventTrigger(MappingNode mappingSettings, MappingMap data) throws ESException {

        parseCollectionNode(mappingSettings, data.getIndex());//初始化配置

        addData = data.getFields();
        baseColRelationField = data.getColRelationField();

        if (collectionNode != null) {//无相关数据域的 配置
            trigger = collectionNode.getTrigger(Event.INSERT);
        } else {
            trigger = null;
        }
        if (trigger == null) {//如果无相关配置 则即可返回
            return;
        }

        logger.info(data.getIndex() + " index of the trigger start merges:");

        this.collectionIndexName = data.getIndex();//数据集的索引名称
        this.id=data.getId();

        List<EventField> eventFields = trigger.getFieldList();

        getSingleAction();//获取action

        if (null != eventFields && eventFields.size() != 0) {//如果field 为空则不触发
            for (EventField field : eventFields) {
                parseEventField(field);
            }
        }

    }

    /**
     * 解析单个eventField insert 只有一个field
     *
     * @param field
     */
    public void parseEventField(EventField field) {
        List<EventHandle> eventHandles = field.getHandleList();
        if (null != eventHandles && eventHandles.size() != 0) {
            for (EventHandle handle : eventHandles) {
                if (handle.getEvent() == Event.INSERT) {//只处理Insert 节点的事件
                    try {
                        logger.info(handle.getIndexName() + " start merge the index:");
                        parseEventHandle(handle);//开始对操作进行解析
                        logger.info(handle.getIndexName() + " index merge successd!");
                    } catch (Exception e) {
                        logger.error(handle.getIndexName() + " index merge faild!", e);
                    }
                }
            }
        }
    }

    /**
     * 解析触发的合并操作
     *
     * @param handle
     */
    public void parseEventHandle(EventHandle handle) throws Exception {

        String indexName = handle.getIndexName();

        this.collectionColumns = new LinkedHashMap<>();
        this.colRelationField = new HashMap<>();
        List<SourceNode> sourceNodeList = handle.getSourceNodeList();
        Map<String, Object> tempColumns = new HashMap<>();//存放临时属性
        for (SourceNode sourceNode : sourceNodeList) {//解析出需要合并的列及数据
            parseSourceNode(sourceNode, tempColumns);
        }
        //解析索引的配置
        Map<String, Map<String, Object>> indexs = ESMapperCaster.parseIndex(handle, this.collectionColumns, mappingNode.getSchemaName(), this.colRelationField);//原始索引配置
       /* Map<String, Map<String, Object>> newIndex = handleMapping(indexs);*/
        HandleMapping map = new HandleMapping();
        map.setIndexMapping((indexs));
        map.setIndex(checkIndexName(indexName));
        map.setFields(handleData(this.collectionColumns, indexs.get(ESKeyWord.Mapping.mapping.name())));//处理数据
        map.setHandleType(Event.INSERT);
        map.setId(this.id);//设置主键

        this.action.doAction(map);

    }

    /**
     * 解析单个sourceNode 的配置
     *
     * @param sourceNode
     */
    protected void parseSourceNode(SourceNode sourceNode, Map<String, Object> tempColumns) {

        String indexName = sourceNode.getIndexName();
        Map<String, Object> columns = new LinkedHashMap<>();
        Map<String, Object> temp = null;
        Map<String, String> cols = sourceNode.getCols();
        String column = null;
        if (StringUtils.isEmpty(indexName)) {//说明此数据是源数据
            for (Map.Entry<String, String> entry : cols.entrySet()) {
                //添加原本的列名以及对应的值，暂时不做column替换,
                column = entry.getKey();//触发索引的原始列名映射的索引的列名
                String field = mappedFieldName(column);//获取触发数据源表映cloumnName 映射的fieldName：用于获取数据以及ES的mapping配置
                //从map的fields 中取值
                columns.put(field, addData.get(field));
                this.colRelationField.put(this.collectionIndexName + "." + field, entry.getValue());//存储column 与field的对应关系
            }
            temp = this.collectionColumns.get(this.collectionIndexName);
            if (null != temp) {
                columns.putAll(temp);//合并数据
            }
            this.collectionColumns.put(this.collectionIndexName, columns);

        } else {//需要从ES 数据中查询
            String field = null;
            indexName = checkIndexName(indexName);//获取完整的索引名称
            Map<String, Object> querConditions = handConditions(sourceNode.getConditionList(), this.addData, tempColumns);
            List<Map<String, Object>> rows = queryESIndex(indexName, querConditions);
            if (null == rows) {
                return;
            }
            Object value = null;
            for (Map.Entry<String, String> entry : cols.entrySet()) {
                column = entry.getKey();//触发索引的原始列名映射的索引的列名
                field = entry.getValue();//column 映射之后的别名
                if (isTempColumn(column)) {//判断属性是不是过度属性
                    column = hadleTempColumn(column);
                    value = rows.get(0).get(column);//取第一行的数据
                    if (null == value && !sourceNode.getAllowNull()) {//如果值为空，并且设置了不允许为空则抛出异常
                        throw new ESTriggerException(indexName + " index " + column + " field is Null!" + rows.get(0));
                    } else {
                        tempColumns.put(column, value);
                    }
                } else {//如果不是临时属性则直接插入
                    if (FunctionStrategy.isFunction(column)) {//使用函数
                        FunctionStrategy strategy = new FunctionStrategy(column);
                        Object concat = strategy.executerFunction(rows);
                        if (null != concat) {
                            columns.put(strategy.getFunctionCode(), concat);
                            this.colRelationField.put(indexName + "." + strategy.getFunctionCode(), field);//存储column 与field的对应关系
                        }
                    } else {
                        //添加原本的列名以及对应的值，暂时不做column替换
                        columns.put(column, rows.get(0).get(column));//取第一行的数据
                        this.colRelationField.put(indexName + "." + column, field);//存储column 与field的对应关系
                    }
                    this.collectionColumns.put(sourceNode.getIndexName(), columns);
                }
            }
        }

    }

    /**
     * 处理条件并返回相关参数
     *
     * @param conditionList
     * @param data          来源数据
     * @param tempColumns   临时数据
     * @return
     */
    protected Map<String, Object> handConditions(List<Condition> conditionList, Map<String, Object> data, Map<String, Object> tempColumns) {
        Map<String, Object> conditions = new HashMap<>();
        String column = null;
        Object value = null;
        for (Condition condition : conditionList) {
            column = condition.getColumnName();
            if (isTempColumn(column)) {//判断参数是不是临时属性
                //去掉中括号
                column = hadleTempColumn(column);
                value = tempColumns.get(column);//临时数据中获取值
            } else {
                //先从map的映射关系中找出对应的fieldName 再 从data里面取值
                value = data.get(mappedFieldName(column));
                if (null == value) {//如果值为空则从已经查出的数据获取
                    column = getColumnNameByField(this.colRelationField, column);//根据columnName 获取映射的fieldName
                    for (Map.Entry<String, Map<String, Object>> map : this.collectionColumns.entrySet()) {
                        if(column.contains(".")){
                            value = map.getValue().get(column.split("\\.")[1]);
                        }else{
                            value = map.getValue().get(column);
                        }
                        if (value != null) {
                            break;
                        }
                    }
                }
            }
            conditions.put(condition.getFieldName(), value);
        }
        return conditions;
    }

    /**
     * 查询ES
     *
     * @param indexName  索引名称
     * @param conditions 查询条件
     * @return
     */
    private List<Map<String, Object>> queryESIndex(String indexName, Map<String, Object> conditions) {
        List<Map<String, Object>> result = transportClientService.mustQueryByCondition(indexName, conditions);
        if (null != result && result.size() != 0) {
            return result;
        } else {
            logger.warn(indexName + " index query of the result is Null，query conditions of:" + conditions);
            return null;
        }
    }

    /**
     * 将columnName 替换成fieldName
     *
     * @param indexs
     */
    private Map<String, Map<String, Object>> handleMapping(Map<String, Map<String, Object>> indexs) {
        Map<String, Map<String, Object>> newIndex = indexs;
        Map<String, Object> mappings = indexs.get(ESKeyWord.Mapping.mapping.name());
        Map<String, Object> newMappings = new TreeMap<>();
        for (Map.Entry<String, Object> entry : mappings.entrySet()) {
            newMappings.put(this.colRelationField.get(entry.getKey()), entry.getValue());
        }
        newIndex.put(ESKeyWord.Mapping.mapping.name(), newMappings);
        return newIndex;
    }

    /**
     * 将不同索引的列进行合并，并替换field关系
     *
     * @param collectionColumns 合并索引，每个索引下面的列属性及其值
     * @param mappings          映射
     */
    private Map<String, Object> handleData(Map<String, Map<String, Object>> collectionColumns, Map<String, Object> mappings) {
        Map<String, Map<String, Object>> indexColumns = collectionColumns;
        Map<String, Object> newData = new TreeMap<>();
        String field = null;
        Object value = null;
        for (Map.Entry<String, Map<String, Object>> index : indexColumns.entrySet()) {
            for (Map.Entry<String, Object> entry : index.getValue().entrySet()) {
                field = this.colRelationField.get(index.getKey() + "." + entry.getKey());
                value = entry.getValue();
                Map<String, Object> fieldMapping = (Map<String, Object>) mappings.get(field);

                if (MappingType.NESTED.getCode().equals(fieldMapping.get(ESKeyWord.Mapping.type.name()).toString())) {//判断是不是nested类型
                    if (value instanceof String) {//如果是String 类型
                        value = JSON.parse(value.toString());
                    }
                }
                newData.put(field, value);
            }
        }
        return newData;
    }


    /**
     * 校验属性是否是临时属性
     *
     * @param column
     * @return
     */
    private Boolean isTempColumn(String column) {
        if (column.matches("\\[.+\\]")) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    /**
     * 通过 field 获取column
     *
     * @param relations column映射field的关系
     * @param fileName  field
     * @return
     */
    private String getColumnNameByField(Map<String, String> relations, String fileName) {
        for (Map.Entry<String, String> entry : relations.entrySet()) {
            if (entry.getValue().equals(fileName)) {
                return entry.getKey();
            }
        }
        return fileName;
    }

    /**
     * 通过column 找到对应的映射关系的fieldName
     *
     * @param columnName
     * @return
     */
    private String mappedFieldName(String columnName) {
        String fieldName = this.baseColRelationField.get(columnName);
        if (StringUtils.isEmpty(fieldName)) {
            return columnName;
        }
        return fieldName;
    }

    /**
     * 处理临时属性
     *
     * @param column
     * @return
     */
    private String hadleTempColumn(String column) {
        return column.substring(1, column.length() - 1);
    }


}
