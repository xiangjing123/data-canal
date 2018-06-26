package com.tmg.internship.datacanal.escenter.moduls.mapping.cast;

import com.tmg.internship.datacanal.escenter.common.ESMappingUtil;
import com.tmg.internship.datacanal.escenter.exception.MappingException;
import com.tmg.internship.datacanal.escenter.executer.AbstractExecuter;
import com.tmg.internship.datacanal.escenter.moduls.config.*;
import com.tmg.internship.datacanal.escenter.moduls.index.MappingMap;
import com.tmg.internship.datacanal.escenter.moduls.mapping.MappingCaster;

import java.util.Map;
import java.util.TreeMap;

/**
 * 基本抽象铸造器，包含对settings，和aliases 的实现
 *
 * @author xiangjing
 * @date 2018/5/9
 * @company 天极云智
 */
public abstract  class BasicAbstractCaster implements MappingCaster {

    /**
     * 默认使用中文分词的 名称
     */
    private static String default_name = "school_name,department_name,class_name,student_name," +
            "create_name,contact_name,read_name,score_name,audit_name,addr,operator_name," +
            "real_name,county_name,city_name,province_name";

    //数据库名和表名连接符
    protected static final String default_connect = AbstractExecuter.default_connect;

    //Es mapping 配置
    protected ESConfigure configure;

    //数据库名和表名连接符
    public static final String default_aliase = "$";

    //数据域
    protected MappingNode mappingNode;

    //数据集
    protected CollectionNode collectionNode;

    @Override
    public void setESConfigure(ESConfigure esConfigure) {
        configure = esConfigure;
    }

    /**
     * 通过值使用默认（默认请参考Mysql 数据转ES 相关文档）类型映射，返回ES相应的类型
     * @param value
     * @return
     */
    @Override
    public MappingType defaultType(Object value) {
        if(null !=value){
            String val = value.toString();
            if(ESMappingUtil.isLong(val)){
                return MappingType.LONG;
            }else if(ESMappingUtil.isDouble(val)){
                return MappingType.DOUBLE;
            }else if(ESMappingUtil.isJSON(val)){
                if(ESMappingUtil.isJSONObject(val) ||ESMappingUtil.isJSONArrayObject(val)){//如果是Object类型的和object数组类型的则返回nested 类型的
                    return MappingType.NESTED;
                }
                return MappingType.TEXT;
            }
        }
        return MappingType.TEXT;
    }

    /**
     * 使用约定（约定请参考Mysql 数据转ES 相关文档）来进行类型映射 如果未约定则返回null
     * @param key
     * @return
     */
    @Override
    public MappingType typeAppoint(String key,Object value) {
        if(null !=value && value.toString().length()!=0){//时间约定
            if(ESMappingUtil.isDate(value.toString())){
                return MappingType.DATE;
            }
            String upkey= key.toLowerCase();//忽略大小写

            if(upkey.equals("status")){
                return MappingType.SHORT;
            }
            if(key.matches("[a-z,A-Z,$,_,\\d]*(title|TITLE)$")){//以text结尾的内容默认为text
                return MappingType.TEXT;
            }
            if(key.matches("[a-z,A-Z,$,_,\\d]*(content|CONTENT)$")) {//以content 结尾的内容默认为text
                return MappingType.TEXT;
            }
            if(key.matches("[a-z,A-Z,$,_,\\d]*(user_id|USER_ID)$")){//以user_id 结尾的内容默认为text
                return MappingType.TEXT;
            }
            if(key.matches("[a-z,A-Z,$,_,\\d]*(name|NAME)$")){//以name结尾的内容默认为text
                return MappingType.TEXT;
            }
            if(key.matches("[a-z,A-Z,$,_,\\d]*(description|DESCRIPTION)$")){//以description 结尾的内容默认为text
                return MappingType.TEXT;
            }
            if(upkey.equals("creator")){
                return MappingType.KEYWORD;
            }
            if(upkey.equals("updator")){
                return MappingType.KEYWORD;
            }
            if(upkey.equals("node_id")){
                return MappingType.LONG;
            }
            if(upkey.equals("manage_id")){
                return MappingType.KEYWORD;
            }
            if(upkey.equals("res_id")){
                return MappingType.KEYWORD;
            }
            if(upkey.equals("audit_user")){
                return MappingType.KEYWORD;
            }
            if(upkey.equals("student_id")){
                return MappingType.KEYWORD;
            }
        }
        return null;
    }

    /**
     * 使用约定（约定请参考Mysql 数据转ES 相关文档）来进行分词器的设置
     * @param key
     * @return
     */
    @Override
    public Tokenizer tokenizerAppoint(String key) {

        String upkey= key.toLowerCase();//忽略大小写

        if(upkey.contains("path") ){
            return Tokenizer.PATH;
        }else {
            String[] names= this.default_name.split(",");
            for(String name:names){
                if(name.equals(key)){
                    return Tokenizer.IK_SMART;
                }
            }
        }
        return Tokenizer.STANDARD;
    }
    /**
     * 设置别名
     *
     * @param map
     * @return
     * @throws MappingException
     */
    @Override
    public Map<String, Object> createAliases(MappingMap map) throws MappingException {
        return new TreeMap<String, Object>() {{
            put(map.getIndex().replace(default_connect, default_aliase), "{}");
        }};
    }

    /**
     * 解析ES 映射的数据类型
     * @param key
     * @param value
     * @return
     */
    protected String parserType(String key,Object value){
        MappingType type = typeAppoint(key,value);//使用约定的数据类型
        if(null == type){//未有约定的类型
            type =  defaultType(value);//使用默认类型
        }
        return  type.getCode();
    }

    /**
     * 解析ES 属性分词类型
     * @param key
     * @return
     */
    protected Tokenizer parseTokenier(String key){
        Tokenizer tokenizer = this.mappingNode.getWholeTokenizer(key);//第二顺序采用全局mapping设置
        if(null != tokenizer){//
            return tokenizer;
        }
        tokenizer= tokenizerAppoint(key);//使用约定
        return  tokenizer;
    }

    @Override
    public void setMappingNodeSetting(MappingNode mappingNode) {
        this.mappingNode = mappingNode;
    }

    @Override
    public void setCollectionNodeSetting(CollectionNode collectionNode) {
        this.collectionNode =collectionNode;
    }
}
