package com.tmg.internship.datacanal.escenter.moduls.index;
import com.alibaba.fastjson.JSONObject;
import com.tmg.internship.datacanal.escenter.common.ESMappingUtil;
import com.tmg.internship.datacanal.escenter.moduls.config.CollectionNode;
import com.tmg.internship.datacanal.escenter.moduls.config.ConcatParam;
import com.tmg.internship.datacanal.escenter.moduls.config.FieldNode;
import com.tmg.internship.datacanal.escenter.moduls.config.MappingRule;
import com.tmg.internship.datacanal.escenter.parser.Event;
import com.tmg.internship.datacanal.escenter.parser.EventData;
import com.tmg.utils.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * 按照配置文件处理数据
 *
 * @author xiangjing
 * @date 2018/5/14
 * @company 天极云智
 */
public class ConfigureHandleData implements  MQHandleData {

    //数据集的节点配置
    protected CollectionNode collectionNode;

    //某个数据集下面的具体配置
    protected static FieldNode fieldNode =null;
    //field 下面的映射规则
    protected static MappingRule rule= null;

    //column 映射的fieldName 关系
    protected Map<String,String> colRelationField;

    //field 下面的映射规则
    protected static Map<String,Object> map= null;

    public ConfigureHandleData(CollectionNode collectionNode) {
        this.collectionNode = collectionNode;
    }

    public CollectionNode getCollectionNode() {
        return collectionNode;
    }

    public void setCollectionNode(CollectionNode collectionNode) {
        this.collectionNode = collectionNode;
    }


    @Override
    public HandleMapping HandleData(EventData data) {
        HandleMapping mapping = new HandleMapping();
        mapping.setHandleType(data.getEvent());//事件类型转换为操作类型
        mapping.setIndex(data.getIndex().toLowerCase());
        mapping.setData(data);//设置原始数据
        Map<String,Object> columns = getCompeleteData(data);//获取完整的源数据

        this.colRelationField = new HashMap<>();
        mapping.setId(getESId(data.getKeys(),columns));//设置ESid

        mapping.setFields(mappingFileds(columns));//设置完整的数据
        mapping.setColRelationField(this.colRelationField);//设置映射关系

        if(data.getEvent() == Event.UPDATE){
            mapping.setUpdateFields(mappingFileds(data.getAfter()));//设置更新数据的mapping
        }
        return mapping;
    }

    /**
     * 获取完整的数据
     * @param data
     * @return
     */
    protected Map<String,Object> getCompeleteData(EventData data){

        if( data.getEvent() == Event.INSERT ){//添加数据的事件
            return data.getAfter();
        }else if( data.getEvent() == Event.DELETE ){//删除数据的事件
            return data.getBefore();
        }else{//更新数据的事件
            Map<String,Object> map = new HashMap<>();
            map.putAll(data.getBefore());
            map.putAll(data.getAfter());

            return map;
        }
    }

    /**
     * 获取索引id
     * @param ids
     * @param columns 完整的数据
     * @return
     */
    protected String getESId(List<String> ids,Map<String,Object> columns){

        if( null != ids && ids.size() != 0){
            StringBuffer sb =new StringBuffer();
            ids.stream().sorted().forEach(id->sb.append(columns.get(id)+"_"));
            String id = sb.toString();
            return  id.substring(0,id.length()-1);
        }else{
            return null;
        }

    }

    /**
     * 将源数据转换为对应的ES 数据
     * @param columns
     * @return
     */
    protected Map<String,Object> mappingFileds(Map<String,Object> columns){
        Map<String,Object> fields =Collections.synchronizedMap(new HashMap<String,Object>()) ;//完整的映射过后的数据
        for(String key:columns.keySet()){//基本数据

            Map<String,Object> temp = mappingField(key, columns);
            if( null != temp ){//只有当value值不为空的时候才会添加
                fields.putAll(temp);
            }
        }
        handleRedundanceField(columns, fields);//添加冗余映射
        return fields;
    }

    /**
     * 通过columnName 返回filedName 属性
     * @param columnName
     * @param compelete
     * @return
     */
    protected  Map<String,Object> mappingField(String columnName,Map<String,Object> compelete){
        map = new HashMap<>();
        String key = columnName;
        Object value =  compelete.get(columnName);
        if( null == value){
            return null;
        }else{
            if(value.toString().length() == 0 || "null".equalsIgnoreCase(value.toString())){//如果值为null 则不添加
                return  null;
            }
        }
        if(null != this.collectionNode){

            fieldNode = collectionNode.getFieldNodeByColumn(columnName);
        }else{
            fieldNode = null;
        }

        if(null !=  fieldNode){//如果为空，则没有相关配置，则使用默认配置,不为空，则使用一下xml 配置
            if(!StringUtils.isTrimEmpty(fieldNode.getFiledName())){//如果fieldName 为空 则使用columnName
                this.colRelationField.put(key,fieldNode.getFiledName());//存储columnName 对应 fieldName的映射关系
                key = fieldNode.getFiledName();
            }
            rule = fieldNode.getMappingRule();
            if( null != rule ){
                List<MappingRule.Replace> replaces=rule.getReplaceAll();
                if(null != replaces && replaces.size() !=0){
                    for(MappingRule.Replace replace:replaces){
                        value =replaceRule(value.toString(),replace);
                    }
                }
            }
        }

        if(ESMappingUtil.isJSON(value.toString())){
            value = JSONObject.parse(value.toString());
        }

        map.put(key,value);
        return map;
    }

    /**
     * 安装xml 配置文件为冗余的filedName 处理数据
     * @param columns 源数据
     * @param fields 映射过后的基本数据
     */
    protected void handleRedundanceField(Map<String, Object> columns, Map<String, Object> fields){

        if(null != collectionNode){
            List<FieldNode> fieldNodes = this.collectionNode.getFieldNodeList();
            if(null !=  fieldNodes && fieldNodes.size() !=0){//如果为空，则没有相关配置，则使用默认配置,不为空，则使用一下xml 配置
                for(FieldNode fieldNode:fieldNodes){
                    if(!StringUtils.isTrimEmpty(fieldNode.getFiledName()) && StringUtils.isTrimEmpty(fieldNode.getColumnName())){//当fieldName 不为空，columnName 不为空的时候则表示是冗余字段
                        List<ConcatParam> concat=fieldNode.getMappingRule().getConcat();
                        String fieldValue= concatRule(concat,columns,fields);
                        fields.put(fieldNode.getFiledName(),fieldValue);
                        this.colRelationField.put(fieldNode.getFiledName(),fieldNode.getFiledName());//存储column->fileName的映射关系

                    }
                }
            }
        }
    }


    /**
     * 正则替换
     * @param columnValue column 对应的值
     * @param replace
     * @return
     */
    @Override
    public String replaceRule(String columnValue,MappingRule.Replace replace) {
        if(StringUtils.isTrimEmpty(columnValue)){
            return columnValue;
        }else{
            return columnValue.replaceAll(replace.getRegex(),replace.getReplacement());
        }

    }

    /**
     * 数据拼接
     * @param params 连接的参数
     * @param columns 源数据
     * @param fields 映射过后的数据
     * @return
     */
    @Override
    public String concatRule(List<ConcatParam> params, Map<String, Object> columns, Map<String, Object> fields) {
        if(null != params && params.size() != 0){
            StringBuffer sb =new StringBuffer();
            for(ConcatParam param:params){
                if(param.getType() == ConcatParam.ParamType.COMMON){
                    sb.append(param);
                }else if(param.getType() == ConcatParam.ParamType.COLUMN){
                    sb.append(columns.get(param));
                }else{
                    sb.append(fields.get(param));
                }
            }
            return sb.toString();
        }else{
            return "";
        }
    }

    @Override
    public void setCollectionNodeSetting(CollectionNode collectionNode) {
        this.collectionNode=collectionNode;
    }

}
