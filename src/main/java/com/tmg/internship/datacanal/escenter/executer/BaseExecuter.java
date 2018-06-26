package com.tmg.internship.datacanal.escenter.executer;

import com.tmg.internship.datacanal.escenter.exception.ESTriggerException;
import com.tmg.internship.datacanal.escenter.moduls.config.ESConfigure;
import com.tmg.internship.datacanal.escenter.moduls.index.AppointJSONHandle;
import com.tmg.internship.datacanal.escenter.moduls.index.HandleMapping;
import com.tmg.internship.datacanal.escenter.moduls.index.MQHandleData;
import com.tmg.internship.datacanal.escenter.moduls.mapping.*;
import com.tmg.internship.datacanal.escenter.moduls.mapping.cast.BasicMappingCaster;
import com.tmg.internship.datacanal.escenter.parser.EventData;
import com.tmg.internship.datacanal.escenter.trigger.TriggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基本的索引执行理器
 *
 * @author xiangjing
 * @date 2018/5/7
 * @company 天极云智
 */
public class BaseExecuter extends AbstractExecuter {

    public static final Logger logger = LoggerFactory.getLogger(BaseExecuter.class);

    /**
     * 映射器接口
     */
    private Mapper mapper;

    /**
     * 数据处理接口
     */
    private MQHandleData mqHandleData;

    /**
     * 映射器构造器
     */
    private MappingCaster mappingCaster;

    /**
     * ES 直接操作对象
     */
    private ESAction action;


    public BaseExecuter(ESConfigure configure) {
        super(configure);

    }

    /**
     * 单例模式
     */
    private void getSingleAction(){
        if( null == this.action){
            this.action = new BaseESAction();
        }
    }

    /*
     * 准备环境
     */
    public void readyApp(String index){

        parseCollectionConfig(index);//解析对应数据域的配置

        if (null == mqHandleData) {
            mqHandleData = new AppointJSONHandle(this.collectionNode);//实例化数据解析对象
        } else {
            mqHandleData.setCollectionNodeSetting(this.collectionNode);
        }

        if (null == mappingCaster) {//如果为空则实例化对象
            mappingCaster = new BasicMappingCaster(this.configure,this.mappingNode, this.collectionNode);//实例mapper构造器

        } else {//如果不为空则改变参数
            mappingCaster.setCollectionNodeSetting(this.collectionNode);
            mappingCaster.setMappingNodeSetting(this.mappingNode);
        }

        if (null == mapper) {
            mapper = new BaseMapper(this.mappingCaster);//实例化mapper 映射器
        }else{
            mapper.setMappingCaster(mappingCaster);
        }

        getSingleAction();


    }

    @Override
    public void execute(EventData eventData) throws Exception {
        logger.info(eventData.getIndex()+" index data start proccesser!");

        readyApp(eventData.getIndex());//准备环境

        HandleMapping handleMap = mqHandleData.HandleData(eventData);//将源数据转换为对应的es数据

        handleMap.setIndex(getIndexName(handleMap.getIndex()));//获取索引名称

        mapper.parseMapping(handleMap);//解析映射关系

        try{
            this.action.doAction(handleMap);//比较索引，push 数据

            TriggerFactory.getESTrigger(handleMap.getHandleType()).eventTrigger(this.mappingNode, handleMap);//触发器
        }catch (Exception e){
            if(e instanceof ESTriggerException){
                logger.error(handleMap.getIndex() +" Trigger trigger faild!",e);
            }else{
                logger.error(eventData.getIndex()+" index proccessor data faild!"+e);
            }
            //throw  new ESException(e);
        }
    }

    /**
     * 获取索引别名
     *判断是不是存在配置 如果存在配置，则使用配置的索引名称
     *判断索引名称是不是完整的索引名称，如果是则返回，如果不是则拼接
     * @param index mq 来源数据的topic
     * @return
     */
    private String getIndexName(String index) {
        if(null != this.collectionNode){
            String indexName = this.collectionNode.getIndexName();
            if(indexName.contains(default_connect)){
                return indexName;
            }else{
                return this.mappingNode.getSchemaName()+default_connect+indexName;
            }
        }else{
            return index;
        }
    }

    public Mapper getMapper() {
        return mapper;
    }

    public void setMapper(Mapper mapper) {
        this.mapper = mapper;
    }

    public MQHandleData getMqHandleData() {
        return mqHandleData;
    }

    public void setMqHandleData(MQHandleData mqHandleData) {
        this.mqHandleData = mqHandleData;
    }

    public MappingCaster getMappingCaster() {
        return mappingCaster;
    }

    public void setMappingCaster(MappingCaster mappingCaster) {
        this.mappingCaster = mappingCaster;
    }
}
