package com.tmg.internship.datacanal.escenter.executer;

import com.tmg.commons.utils.SpringUtils;
import com.tmg.internship.datacanal.escenter.esengine.service.HighLevelClientService;
import com.tmg.internship.datacanal.escenter.exception.ESException;
import com.tmg.internship.datacanal.escenter.moduls.index.HandleMapping;
import com.tmg.internship.datacanal.escenter.moduls.mapping.BaseMapper;
import com.tmg.internship.datacanal.escenter.moduls.mapping.ESKeyWord;
import com.tmg.internship.datacanal.escenter.moduls.mapping.IndexChange;
import com.tmg.internship.datacanal.escenter.moduls.mapping.Mapper;
import com.tmg.internship.datacanal.escenter.moduls.notice.AsynSendSMS;
import com.tmg.internship.datacanal.escenter.parser.Event;
import com.tmg.utils.redis.SimpleRedisTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.TreeMap;

/**
 * ES中心操作接口实现
 *
 * @author xiangjing
 * @date 2018/6/11
 * @company 天极云智
 */
public class BaseESAction implements ESAction {

    public static final Logger logger = LoggerFactory.getLogger(BaseESAction.class);

    /**
     * 索引版本
     */
    public static final String index_version = "_version";

    protected static Mapper mapper;

    /**
     * 异步消息发送器
     */
    protected static AsynSendSMS sendSMS;

    /**
     * ES 引擎
     */
    protected static HighLevelClientService clientService;
    /**
     * redis 工具类
     */
    protected SimpleRedisTool simpleRedisTool = new SimpleRedisTool();


    static {
        sendSMS = SpringUtils.getContext().getBean(AsynSendSMS.class);
        clientService = SpringUtils.getContext().getBean(HighLevelClientService.class);
        mapper = new BaseMapper();
    }


    @Override
    public void doAction(HandleMapping handleMap) throws Exception {
        //比较索引是否一致
        IndexChange indexChange = mapper.checkIndexConfig(handleMap);

        if (indexChange.getShardChange()) {//分片数发生变化
            sendSMS.sendMsg(handleMap.getIndex());
            logger.warn(handleMap.getIndex() + " index of the shards has changed,please to handle On Under line!");
        }

        switch (indexChange.getChangeType()) {
            case NO_CHANGE://映射未变化
                putData(handleMap);
                break;
            case UPDATE://映射或者配置改变
                try{
                    putData(handleMap);
                }catch (Exception e){
                    logger.warn(handleMap.getIndex() + " index of mapping has changed，please to handle On Under line!",e);
                    Integer version = getIndexVersion(handleMap.getIndex());
                    String baseIndex = handleMap.getIndex();
                    if (null != version) {//如果已经有其他版本的
                        version = version + 1;
                    } else {
                        version = 1;
                        sendSMS.sendMsg(handleMap.getIndex());//发送短信通知
                    }
                    handleMap.setIndex(baseIndex + "_" + version);//就重新生成新版本的索引
                    createIndex(handleMap);//创建新版索引
                    cacheIndexVersion(baseIndex, version);//缓存版本信息
                    putData(handleMap);//更新数据
                    handleMap.setIndex(baseIndex);//还原
                    mapper.cacheMap(handleMap);//缓存索引
                }
            case NOT_EXIST://映射不存在
                if (!checkIndexExist(handleMap)) {
                    createIndex(handleMap);//创建索引
                }
                mapper.cacheMap(handleMap);
                putData(handleMap);
                break;
            case SETTING_ADD://添加配置
                addSetting(handleMap.getIndex(), indexChange.getIndexChange().get(ESKeyWord.Setting.settings.name()));
                mapper.cacheMap(handleMap);
                putData(handleMap);//更新数据
                break;
            case MAPPING_ADD://添加映射
                addMapping(handleMap.getIndex(), handleMap.getType(), indexChange.getIndexChange().get(ESKeyWord.Mapping.mapping.name()));
                mapper.cacheMap(handleMap);
                putData(handleMap);
                break;
            case ADD://既添加映射和配置
                addSettingAndMapping(handleMap.getIndex(), handleMap.getType(), indexChange.getIndexChange());
                mapper.cacheMap(handleMap);
                putData(handleMap);
                break;
        }

    }

    /**
     * 添加索引备注
     *
     * @param index    索引名称
     * @param settings 索引配置
     */
    public void addSetting(String index, Map<String, Object> settings) {
        try {
            String newIndex = getNewIndexVersion(index);//校验索引是不是最新版本的索引，如果是则返回新版本的索引
            clientService.closeIndex(newIndex);
            clientService.updateSettings(newIndex, settings);
            clientService.openIndex(newIndex);
        } catch (Exception e) {
            throw new ESException(index + " index add of the seetings faild!", e);
        }

    }

    /**
     * 添加索引映射
     *
     * @param index 索引名称
     * @param type  索引类型
     * @param map   索引列相关映射
     */
    public void addMapping(String index, String type, Map<String, Object> map) {
        try {
            String newIndex = getNewIndexVersion(index);//校验索引是不是最新版本的索引，如果是则返回新版本的索引
            clientService.putMapping(newIndex, type, handleMapping(type, map));
        } catch (Exception e) {
            throw new ESException(index + " index add of the mappings faild!", e);
        }

    }

    /**
     * 添加索引配置以及索引映射
     *
     * @param index
     * @param type
     * @param maps
     */
    public void addSettingAndMapping(String index, String type, Map<String, Map<String, Object>> maps) {
        addSetting(index, maps.get(ESKeyWord.Setting.settings.name()));
        addMapping(index, type, maps.get(ESKeyWord.Mapping.mapping.name()));//映射节点只到properties 节点下面的属性层次
    }

    /**
     * push 数据到es
     *
     * @param data
     */
    public void putData(HandleMapping data) throws IOException {
        try {
            String index = getNewIndexVersion(data.getIndex());//校验索引是不是最新版本的索引并返回最新索引
            if (data.getHandleType() == Event.DELETE) {
                clientService.delete(index, data.getType(), data.getId());
            } else if (data.getHandleType() == Event.UPDATE) {
                clientService.updateDocument(index, data.getType(), data.getId(), data.getUpdateFields());
            } else {//添加数据
                clientService.putDocumentWithId(index, data.getType(), data.getId(), data.getFields());
            }
        } catch (Exception e) {
            throw new ESException(data.getIndex() + " index " + data.getHandleType().name() + " the data faild!", e);
        }
    }

    /**
     * 创建索引
     *
     * @param data
     */
    public void createIndex(HandleMapping data) throws IOException {
        Map<String, Object> aliase = data.getIndexMapping().get(ESKeyWord.aliases.aliases.name());//完整的别名，需要提出key
        Map<String, Object> settings = data.getIndexMapping().get(ESKeyWord.Setting.settings.name());
        Map<String, Object> map = data.getIndexMapping().get(ESKeyWord.Mapping.mapping.name());//待处理
        Map<String, Object> mapping = handleMapping(data.getType(), map);//处理mapping
        String aliases = "";
        for (String key : aliase.keySet()) {
            aliases = key;
        }
        clientService.createIndex(data.getIndex(), settings, data.getType(), mapping, aliases);

    }

    /**
     * 判断索引是否存在
     *
     * @param data
     */
    public boolean checkIndexExist(HandleMapping data) throws IOException {
        return clientService.indexOrDocumentIsExists(data.getIndex(), null, null);
    }

    /**
     * 获取最新版本的索引
     *
     * @param index
     * @return
     * @throws UnsupportedEncodingException
     */
    public String getNewIndexVersion(String index) throws UnsupportedEncodingException {
        Integer version = getIndexVersion(index);
        if (version != null) {//说明索引版本有迭代
            return index + "_" + version;//从新使用新版本的索引
        } else {
            return index;
        }
    }

    /**
     * 添加mapping 的父节点
     *
     * @param type 类型
     * @param map  field 相关映射
     * @return
     */
    protected Map<String, Object> handleMapping(String type, Map<String, Object> map) {
        return new TreeMap<String, Object>() {{
            put(type, new TreeMap<String, Object>() {{
                put(ESKeyWord.Mapping.properties.name(), map);
            }});
        }};
    }

    /**
     * 缓存索引版本
     *
     * @param index
     * @param version
     */
    protected void cacheIndexVersion(String index, int version) throws UnsupportedEncodingException {
        simpleRedisTool.setObject(index + index_version, version);
    }

    /**
     * 获取索引版本
     *
     * @param index
     * @return
     */
    protected Integer getIndexVersion(String index) throws UnsupportedEncodingException {
        return simpleRedisTool.getObject(index + index_version, Integer.class);
    }
}
