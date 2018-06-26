package com.tmg.internship.datacanal.escenter.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.tmg.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * 映射到ES 的工具类
 *
 * @author xiangjing
 * @date 2018/5/9
 * @company 天极云智
 */
public class ESMappingUtil {

    public static  final Logger logger = LoggerFactory.getLogger(ESMappingUtil.class);


    /**
     * 解析json 格式的字符串
     * @param json
     * @return
     */
    public static Object parseJSON(String json){
        if(StringUtils.isTrimEmpty(json)){
            return null;
        }
        try{
            return JSON.parse(json);
        }catch (Exception e){
            return null;
        }
    }

    /**
     * 判断字符串是不是JSON 格式的数据
     * @param json
     * @return
     */
    public static boolean isJSON(String json){
        Object obj =parseJSON(json);
        if( null != obj && (obj instanceof JSONArray || obj instanceof JSONObject)){
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    /**
     * 判断数据是不是JSONObject 类型的字符串
     * @param json
     * @return
     */
    public static boolean isJSONObject(String json){
        Object object =parseJSON(json);
        if( null != object && (object instanceof JSONObject)){
            return true;
        }
        return false;
    }

    /**
     * 判断数据是不是JSONArray 类型的字符串
     * @param json
     * @return
     */
    public static boolean isJSONArray(String json){
        Object object =parseJSON(json);
        if( null != object && (object instanceof JSONArray)){
            return true;
        }
        return false;
    }

    /**
     * 判断数据是不是对象素组
     * @param json
     * @return
     */
    public static boolean isJSONArrayObject(String json){
        Object object =parseJSON(json);
        return isJSONArrayObject(object);
    }

    /**
     * 判断数据是不是对象数组
     * @param object
     * @return
     */
    public static boolean isJSONArrayObject(Object object){
        if( null != object && (object instanceof JSONArray)){
            JSONArray array = (JSONArray) object;
            if(array.size() !=0){
                for(Object obj:array){
                    if(!isJSONObject(obj.toString())){
                        return false;
                    }
                }
                return true;
            }
            return false;
        }
        return false;
    }

    /**
     * 校验是不是long类型
     * @param value
     * @return
     */
    public static boolean isLong(String value){
        if(StringUtils.isNumber(value)){
            try{
                Long.parseLong(value);
                return true;
            }catch (Exception e){
                logger.debug(value+",{},超出long 类型范围");
                return false;
            }

        }
        return false;
    }

    /**
     * 校验是不是double类型
     * @param value
     * @return
     */
    public static boolean isDouble(String value){
        if(StringUtils.isNumber(value)){
            try{
                Double.parseDouble(value);
                return true;
            }catch (Exception e){
                logger.debug(value+",{},超出double 类型范围");
                return false;
            }
        }
        return false;
    }
    /**
     * 校验是不是日期类型(注意，本方法不解析yyyyMMdd格式的时间)
     * @param date
     * @return
     */
    public static boolean isDate(String date){
        SimpleDateFormat sdf= null;
        DateFormat format=null;
        for(int i=0;i<DateFormat.values().length;i++){
            format=DateFormat.values()[i];
            if(format == DateFormat.YYYYMMDD){//不匹配yyyyMMdd 格式的
                continue;
            }
            sdf = new SimpleDateFormat(format.getCode());
            try {
                sdf.parse(date);
                if(format.getCode().length() >= date.trim().length()){
                    return true;
                }
            } catch (ParseException e) {
                continue;
            }
        }
        return false;
    }

    /**
     * 获取时间格式的字符串
     * @return
     */
    public static String getDateFormat(){
        StringBuffer sb = new StringBuffer();
        for(int i=0;i<DateFormat.values().length;i++){
            sb.append(DateFormat.values()[i].getCode()+"||");
        }
        String format = sb.toString();
        if(!StringUtils.isTrimEmpty(format)){
            return format.substring(0,format.length()-2);
        }else{
            return null;
        }
    }
}
