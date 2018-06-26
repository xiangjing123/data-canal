package com.tmg.internship.datacanal.escenter.moduls.index;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.tmg.internship.datacanal.escenter.common.DateIntervalArray;
import com.tmg.internship.datacanal.escenter.common.DateIntervalMap;
import com.tmg.internship.datacanal.escenter.common.ESMappingUtil;
import com.tmg.internship.datacanal.escenter.moduls.config.CollectionNode;
import com.tmg.internship.datacanal.escenter.moduls.config.MappingRule;
import com.tmg.internship.datacanal.escenter.moduls.mapping.cast.MappingType;
import com.tmg.utils.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 按照约定的方式处理属性为json格式的字符串
 *
 * @author xiangjing
 * @date 2018/5/31
 * @company 天极云智
 */
public class AppointJSONHandle extends ConfigureHandleData {

    /**
     * 时间开始标签
     */
    private static final String start_tag = "start";

    /**
     * 时间结束标签
     */
    private static final String end_tag = "end";

    public AppointJSONHandle(CollectionNode collectionNode) {
        super(collectionNode);
    }

    @Override
    protected Map<String, Object> mappingField(String columnName, Map<String, Object> compelete) {

        map = new HashMap<>();
        String key = columnName;
        Object value = compelete.get(columnName);
        if (null == value) {
            return null;
        } else {
            if (value.toString().length() == 0 || "null".equalsIgnoreCase(value.toString())) {//如果值为null 则不添加
                return null;
            }
        }
        if (null != this.collectionNode) {

            fieldNode = collectionNode.getFieldNodeByColumn(columnName);
        } else {
            fieldNode = null;
        }

        if (null != fieldNode) {//如果为空，则没有相关配置，则使用默认配置,不为空，则使用一下xml 配置
            if (!StringUtils.isTrimEmpty(fieldNode.getFiledName())) {//如果fieldName 为空 则使用columnName
                this.colRelationField.put(key,fieldNode.getFiledName());//存储columnName 对应 fieldName的映射关系
                key = fieldNode.getFiledName();
            }
            rule = fieldNode.getMappingRule();
            if (null != rule) {
                List<MappingRule.Replace> replaces = rule.getReplaceAll();
                if (null != replaces && replaces.size() != 0) {
                    for (MappingRule.Replace replace : replaces) {
                        value = replaceRule(value.toString(), replace);
                    }
                }
            }
        }

        if (fieldNode != null) {//如果没有配置文件或者说当配置文件
            String mappingType = fieldNode.getMappingType();
            //未设置参数或者参数设置为nested 类型才会去解析为JSON 对象
            if (StringUtils.isEmpty(mappingType) || MappingType.NESTED.getCode().equals(mappingType)) {
                if (ESMappingUtil.isJSON(value.toString())) {//转换时间区间的json
                    value = defaultHandleJson(value.toString());
                }
            }
        } else {
            if (ESMappingUtil.isJSON(value.toString())) {//转换时间区间的json
                value = defaultHandleJson(value.toString());
            }
        }

        map.put(key, value);
        return map;
    }

    /**
     * 按照约定的方式处理json 属性的数据 如果是时间区间的数据则
     * 按照 {"start":"2018-05-31","end":"2018-06-30"} 格式进行返回
     * 如果不是则返回原来的值
     *
     * @param value
     * @return
     */
    private Object defaultHandleJson(String value) {
        Object object = ESMappingUtil.parseJSON(value);
        if (object instanceof JSONObject) {//如果数据是jsonObject对象则返回JSONObject 对象
            return object;
        } else if (object instanceof JSONArray) {//只处理array类型的数据如果不是array类型的数据则返回原来的值
            if (ESMappingUtil.isJSONArrayObject(object)) {//如果本身是jsonObject 数据类型则不用管
                return object;
            } else {//如果是list 或者嵌套list 则判断是不是时间区间的数据
                Object obj = getIntervalData((JSONArray) object);
                if (obj == null) {//
                    return value;
                }
                return obj;
            }
        } else {//如果数据不是jsonObject对象则返回String
            return value;
        }
    }

    /**
     * 判断是不是时间区间，如果是时间区间则按{"start":"2018-05-31","end":"2018-06-30"}返回
     *
     * @param array
     * @return
     */
    private Object getIntervalData(JSONArray array) {
        DateIntervalArray list = new DateIntervalArray();
        JSONObject json = null;
        for (Object obj : array) {
            if (obj instanceof JSONArray) {
                json = checkIntervalDate((JSONArray) obj);//判断其是不是时间区间类型的数据
                if (null == json) {
                    return null;
                }
                list.add(json);
            }
        }
        if (list.size() != 0) {//说明里面的内容是 JSONObject 或者JSONArray类型的数据
            return list;
        } else {
            return checkIntervalDate((array));
        }
    }

    /**
     * 校验jsonObject 是否是["2018-05-31","2018-06-30"]类型的数据格式
     * 如果是则返回{"start":"2018-05-31","end":"2018-06-30"}，
     * 如果不是则返回null
     *
     * @param array
     * @return
     */
    private DateIntervalMap checkIntervalDate(JSONArray array) {
        if (array.size() == 2) {
            DateIntervalMap result = new DateIntervalMap();
            for (int i = 0; i < array.size(); i++) {
                if (ESMappingUtil.isDate(array.getString(i))) {
                    if (i == 0) {
                        result.put(start_tag, array.getString(i));
                    } else if (i == 1) {
                        result.put(end_tag, array.getString(i));
                    }
                } else {
                    return null;
                }

            }
            return result;
        }
        return null;
    }

    /**
     * 递归转换json
     *
     * @param param
     * @return
     */
    @Deprecated
    public static Object recursionParseJSON(String param) {
        Object object = ESMappingUtil.parseJSON(param);
        recursionParseJSON(object);
        return object;
    }

    /**
     * 递归转换json
     *
     * @param obj 参数object 初始为JSONArray 或者JSONObject
     * @return
     */
    @Deprecated
    public static Object recursionParseJSON(Object obj) {
        if (obj instanceof JSONArray) {
            for (Object value : (JSONArray) obj) {
                if (null != value) {
                    recursionParseJSON(value);
                }
            }
        } else if (obj instanceof JSONObject) {
            for (Map.Entry<String, Object> entry : ((JSONObject) obj).entrySet()) {
                entry.setValue(recursionParseJSON(entry.getValue()));
            }
        }
        if (ESMappingUtil.isJSON(obj.toString())) {
            return JSON.parse(obj.toString());
        } else {
            return obj;
        }

    }
}
