package com.tmg.internship.datacanal.escenter.trigger;

import com.sun.istack.internal.NotNull;
import com.tmg.commons.utils.SpringUtils;
import com.tmg.internship.datacanal.escenter.esengine.service.HighLevelClientService;
import com.tmg.internship.datacanal.escenter.esengine.service.TransportClientService;
import com.tmg.internship.datacanal.escenter.executer.AbstractExecuter;
import com.tmg.internship.datacanal.escenter.executer.BaseExecuter;
import com.tmg.internship.datacanal.escenter.moduls.config.CollectionNode;
import com.tmg.internship.datacanal.escenter.moduls.config.MappingNode;
import com.tmg.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 抽象的事件执行器
 *
 * @author xiangjing
 * @date 2018/6/1
 * @company 天极云智
 */
public abstract class AbstractESTrigger implements ESTrigger {

    public Logger logger = LoggerFactory.getLogger(ESTrigger.class);

    /**
     * 数据库名和表名连接符
     */
    public static final String DEFAULT_CONNECT = AbstractExecuter.default_connect;

    /**
     * 分隔后长度
     */
    protected static final int SPLIT_LENGTH = 2;

    /**
     * where条件连接符
     */
    protected static final String WHERE_CONNECT = "=";

    /**
     * 要更新的索引和字段之间的连接符
     */
    protected static final String FILE_ID_CONNECT = "\\.";

    /**
     * 数据集的配置
     */
    protected static CollectionNode collectionNode = null;

    /**
     * 数据域的配置
     */
    protected static  MappingNode mappingNode =null;

    /**
     * ES 引擎
     */
    protected static HighLevelClientService clientService;

    protected static TransportClientService transportClientService;

    static {//初始化引擎
        clientService = SpringUtils.getContext().getBean(HighLevelClientService.class);
        transportClientService=SpringUtils.getContext().getBean(TransportClientService.class);
    }

    /**
     * 获取表名
     *
     * @param index
     * @return
     */
    protected String getCollectionName(String index) {
        return index.split(DEFAULT_CONNECT)[1];
    }

    /**
     * 获取数据域名称（对应数据库的库名）
     *
     * @param index
     * @return
     */
    protected String getMappingName(String index) {
        return index.split(DEFAULT_CONNECT)[0];
    }

    /**
     * 解析 collectionNode 节点的配置
     *
     * @param mappingSetting
     * @param index
     */
    protected void parseCollectionNode(MappingNode mappingSetting, String index) {
        mappingNode =mappingSetting;
        if (mappingSetting != null) {
            collectionNode = mappingSetting.getCollectionNode(getCollectionName(index));
        }
    }

    /**
     * 判断索引名称是不是完整的索引名称：
     * 如果包含default_connect 则表示是完整的值，如果不包含吗，则返回完整的值
     * @param name
     * @return
     */
    protected String  checkIndexName(String name){
        if(name.contains(DEFAULT_CONNECT)){
            return name;
        }else{
            if( null != mappingNode ){
                return mappingNode.getSchemaName()+DEFAULT_CONNECT+name;
            }else{
                return name;
            }
        }
    }

    /**
     * 获取where条件的列
     *
     * @param whereStr
     * @return
     */
    protected String getWhereColum(String whereStr){
        if(StringUtils.isTrimEmpty(whereStr) || whereStr.split(WHERE_CONNECT).length != SPLIT_LENGTH){
            return null;
        }
        return whereStr.split(WHERE_CONNECT)[0];
    }

    /**
     * 获取where条件的值
     *
     * @param whereStr
     * @return
     */
    protected String getWhereValue(String whereStr){
        if(StringUtils.isTrimEmpty(whereStr) || whereStr.split(WHERE_CONNECT).length != SPLIT_LENGTH){
            return null;
        }
        return whereStr.split(WHERE_CONNECT)[1];
    }

    /**
     * 获取要更新的索引
     *
     * @param fileId
     * @return
     */
    protected String getIndexName(@NotNull String fileId) {
        String[] strings = fileId.split(FILE_ID_CONNECT);
        if (strings.length != SPLIT_LENGTH) {
            return null;
        }
        return fileId.split(FILE_ID_CONNECT)[0];
    }

    /**
     * 获取要更新的列
     *
     * @param fileId
     * @return
     */
    protected String getIndexFileId(@NotNull String fileId) {
        String[] strings = fileId.split(FILE_ID_CONNECT);
        if (strings.length != SPLIT_LENGTH) {
            return null;
        }
        return fileId.split(FILE_ID_CONNECT)[1];
    }

    /**
     * 判断update里面的condition的FieldName是否包含"."，如果包含，返回"."之后的字符串，如果不包含，返回全部
     *
     * @param fileId
     * @return
     */
    protected String checkConditionField(String fileId){
        if(fileId.contains(FILE_ID_CONNECT)){
            return fileId.split(FILE_ID_CONNECT)[1];
        }else{
           return fileId;
        }
    }
}
