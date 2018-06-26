package com.tmg.internship.datacanal.escenter.common;

import com.tmg.internship.datacanal.escenter.moduls.config.Tokenizer;
import com.tmg.internship.datacanal.escenter.moduls.mapping.cast.MappingType;
import com.tmg.internship.datacanal.escenter.parser.Event;
import com.tmg.internship.datacanal.escenter.trigger.function.FunctionType;
import com.tmg.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 枚举工具类
 *
 * @author xiangjing
 * @date 2018/5/8
 * @company 天极云智
 */
public class EnumUtil {

    public  static  final Logger logger= LoggerFactory.getLogger(EnumUtil.class);

    /**
     * 将字符串类型转为Event 枚举类型
     * @param event
     * @return
     */
    public static Event parseEvent(String event){
        for(int i=0;i<Event.values().length;i++){
            if(Event.values()[i].name().equalsIgnoreCase(event)){
                return Event.values()[i];
            }
        }
        return null;
    }

    /**
     * 将字符串类型转为Tokenizer 枚举类型
     * @param tokenizer
     * @return
     */
    public static Tokenizer parseTokenizer(String tokenizer){
        for(int i=0;i<Tokenizer.values().length;i++){
            if(Tokenizer.values()[i].getCode().equals(tokenizer)){
                return Tokenizer.values()[i];
            }
        }
        return null;
    }


    /**
     * 将字符串类型转为mappingType 枚举类型
     * @param mappingType
     * @return
     */
    public static MappingType parseESDataType(String mappingType){
        for(int i=0;i<MappingType.values().length;i++){
            if(MappingType.values()[i].getCode().equals(mappingType)){
                return MappingType.values()[i];
            }
        }
        return null;
    }

    /**
     * 将字符串类型转为 Dateformat 枚举类型
     * @param format
     * @return
     */
    public static DateFormat parseDateFormat(String format){
        for(int i=0;i<DateFormat.values().length;i++){
            if(DateFormat.values()[i].getCode().equals(format)){
                return DateFormat.values()[i];
            }
        }
        return null;
    }

    /**
     * 判断是不函数类型
     * @param functionType 根据函数的枚举类型获取其类型
     * @return
     */
    public static Boolean isFunctionType(String functionType) {
        if(!StringUtils.isEmpty(functionType)){
            FunctionType type = null;
            for(int i=0;i<FunctionType.values().length;i++){
                type = FunctionType.values()[i];
                if(functionType.contains(type.getCode())){
                    return Boolean.TRUE;
                }
            }
        }
        return Boolean.FALSE;
    }
}
