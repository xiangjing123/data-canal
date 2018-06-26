package com.tmg.internship.datacanal.escenter.moduls.config;

import com.tmg.internship.datacanal.escenter.common.DateFormat;
import com.tmg.internship.datacanal.escenter.common.EnumUtil;
import com.tmg.internship.datacanal.escenter.exception.ESConfigParseException;
import com.tmg.internship.datacanal.escenter.moduls.config.event.EventHandle;
import com.tmg.internship.datacanal.escenter.moduls.config.event.Trigger;
import com.tmg.internship.datacanal.escenter.moduls.mapping.cast.MappingType;
import com.tmg.internship.datacanal.escenter.parser.Event;
import com.tmg.utils.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.*;

/**
 * 配置文件解析抽象类
 *
 * @author xiangjing
 * @date 2018/5/11
 * @company 天极云智
 */
public abstract class AbstractConfigureParser implements ConfigParser {


    public static final Logger logger = LoggerFactory.getLogger(DefaultConfigureParser.class);

    /**
     * 默认配置文件
     */
    public static String path = "mapping.xml";

    /**
     * 配置文件默认的根节点
     */
    public static final String default_root_node = "mappings";

    /**
     * 某个mapping 下面数据集的名称
     */
    public static List<CollectionNode> mappingCollectionNodes =new ArrayList<>();

    /**
     * 默认主分片数
     */
    public static final Integer default_number_of_shards = 1;

    /**
     * 默认副分片数
     */
    public static final Integer default_number_of_Replicas = 1;

    /**
     *
     */
    public static ESConfigure configure = null;

    @Override
    public ESConfigure parseConfig() throws Exception {

        logger.info("Start parser es mapping configure file....");

        Document document = getDocument();
        Element root = document.getRootElement();

        if (configure == null) {//如果不存在
            configure = new ESConfigure();
        }

        if (default_root_node.equals(root.getName())) {//判断根节点是否存在

            parseShardsSetting(root);//设置全局分片参数

            Iterator iterator = root.elementIterator("mapping");//迭代mapping 节点

            List<MappingNode> mappingNodes = new ArrayList<>();

            MappingNode mappingNode = null;
            List<CollectionNode> collectionNodes = null;
            CollectionNode collectionNode = null;
            Element mapping = null;
            Element collection = null;
            String schemaName = null;
            String collectionName = null;


            while (iterator.hasNext()) {//循环遍历mapping 节点

                mappingNode = new MappingNode();
                mapping = (Element) iterator.next();
                Iterator collections = mapping.elementIterator("collection");
                collectionNodes = new ArrayList<>();
                while (collections.hasNext()) {//循环遍历 collection节点
                    collectionNode = new CollectionNode();
                    collection = (Element) collections.next();
                    collectionName = collection.elementTextTrim("name");
                    if (StringUtils.isTrimEmpty(collectionName)) {
                        throw new ESConfigParseException(mapping.elementTextTrim("schemaName") + ",数据域下面的数据集有name 为空的数据集");
                    }
                    collectionNode.setName(collectionName);//设置索引名称

                    parseCollectionShardsSetting(collection, collectionNode);//设置数据集的分片

                    collectionNode.setFieldNodeList(parseMapping(collection));//设置ES 索引映射配置
                    collectionNode.setIndexName(collection.elementTextTrim("indexName"));//设置索引名称

                    collectionNode.setTriggers(parseEvent(collection));
                    //设置触发事件的配置
                    collectionNodes.add(collectionNode);


                }

                checkCollectionName(collectionNodes, mapping.elementTextTrim("schemaName"));//校验collection 节点中的名称和indexName 是否同名

                schemaName = mapping.elementTextTrim("schemaName");
                if (StringUtils.isTrimEmpty(schemaName)) {
                    throw new ESConfigParseException(this.getConfigFile() + "mapping 节点 schemaName 为空");
                }
                mappingNode.setSchemaName(schemaName);//设置数据域名称
                mappingNode.setCollectionNodes(collectionNodes);//设置数据集配置

                // 设置 需要使用english 分词的columnName
                mappingNode.setEnglish(getDefaultTokenizer(mapping, Tokenizer.ENGLISH.getCode()));
                // 设置 需要使用ik_smart 分词的columnName
                mappingNode.setIkKSmart(getDefaultTokenizer(mapping, Tokenizer.IK_SMART.getCode()));
                // 设置 需要使用ik_max_word 分词的columnName
                mappingNode.setIkMaxWord(getDefaultTokenizer(mapping, Tokenizer.IK_MAX_WORD.getCode()));
                // 设置 需要使用 path_hierarchy 分词的columnName
                mappingNode.setPathHierarchy(getDefaultTokenizer(mapping, Tokenizer.PATH.getCode()));

                mappingNodes.add(mappingNode);
            }

            checkMappingName(mappingNodes);//校验mapping scheamName 中是否存在冲突

            configure.setMappings(mappingNodes);

        } else {
            throw new ESConfigParseException(this.path + "，{}，配置文件根节点错误");
        }
        logger.info("End of parser es mapping configure file....");
        return configure;
    }

    /**
     * 获取分片配置
     *
     * @param element  xml 节点
     * @param nodeName 节点的名称
     * @return
     */
    public Integer getShardsSetting(Element element, String nodeName) {
        String settings = element.elementTextTrim(nodeName);
        if (StringUtils.isEmpty(settings)) {
            return null;
        } else {
            try {
                return Integer.valueOf(settings);
            } catch (NumberFormatException e) {
                throw new ESConfigParseException(this.path + " 配置文件中 " + nodeName + " 节点数据类型只能为number", e);
            }
        }
    }

    /**
     * 解析全局的分片设置
     *
     * @param root
     */
    public void parseShardsSetting(Element root) {
        Integer numberOfShards = getShardsSetting(root, "numberOfShards");
        Integer numberOfReplicas = getShardsSetting(root, "numberOfReplicas");

        //设置主分片
        if (null == numberOfShards) {
            configure.setNumberOfShards(default_number_of_shards);
        } else {
            configure.setNumberOfShards(numberOfShards);

        }
        //设置副分片
        if (null == numberOfReplicas) {
            configure.setNumberOfReplicas(default_number_of_Replicas);
        } else {
            configure.setNumberOfReplicas(numberOfReplicas);
        }
    }

    /**
     * 解析数据集节点的分片设置
     *
     * @param collectionNode collectionNode 数据集节点
     * @param node           collection 节点对象
     */
    public void parseCollectionShardsSetting(Element collectionNode, CollectionNode node) {
        Integer numberOfShards = getShardsSetting(collectionNode, "numberOfShards");
        Integer numberOfReplicas = getShardsSetting(collectionNode, "numberOfReplicas");

        //设置主分片
        if (null == numberOfShards) {
            node.setNumberOfShards(configure.getNumberOfShards());
        } else {
            node.setNumberOfShards(numberOfShards);

        }
        //设置副分片
        if (null == numberOfReplicas) {
            node.setNumberOfReplicas(configure.getNumberOfReplicas());
        } else {
            node.setNumberOfReplicas(numberOfReplicas);
        }
    }

    /**
     * 加载配置文件生成Document
     *
     * @return
     */
    protected Document getDocument() throws DocumentException {
        SAXReader reader = new SAXReader();
        return reader.read(getConfigFile());
    }

    /**
     * 校验 mapping Name 节点的名称是否相同
     *
     * @param mappingNodes
     * @return
     */
    protected void checkMappingName(List<MappingNode> mappingNodes) {

        Map<String,Integer> names = new HashMap<>();
        mappingNodes.forEach(mapping -> {//校验
            if(names.get(mapping.getSchemaName())!=null){
                throw new ESConfigParseException(this.path + ",{},配置文件 mapping 节点中 schemaName :"+mapping.getSchemaName()+"的 节点重复");
            }else{
                names.put(mapping.getSchemaName(),1);
            }
        });

    }

    /**
     * 校验 collection 节点 name 和 indexName的名称是否存在同名
     *
     * @param collectionNodes
     * @param mappingName
     * @return
     */
    protected void checkCollectionName(List<CollectionNode> collectionNodes, String mappingName) {

        Map<String,Integer> names = new HashMap<>();
        Map<String,Integer> mergeIndexNames = new HashMap<>();
        Trigger trigger = null;
        List<EventHandle> handles =null;
        for(CollectionNode coll:collectionNodes){
            if(names.get(coll.getName())!=null){
                throw new ESConfigParseException(this.path + ",{},配置文件" + mappingName + "数据域下面的 collection 节点中 name 重复:"+coll.getName());
            }else{
                names.put(coll.getName(),1);
                trigger=coll.getTrigger(Event.INSERT);
                if(null !=trigger){
                    handles = trigger.getFieldList().get(0).getHandleList();
                    for(EventHandle handle:handles){
                        if(mergeIndexNames.get(handle.getIndexName())!=null){//校验合并的索引是否重名
                            throw new ESConfigParseException(this.path + ",{},配置文件" + mappingName + "数据域下面的"+coll.getName()+" 数据集下面 triggers onInsert 中 indexName 重复:"+handle.getIndexName());
                        }else{
                            mergeIndexNames.put(handle.getIndexName(),1);
                        }
                    }

                }
            }
        }
        for(String key:mergeIndexNames.keySet()){//校验合并的索引名称和数据集名称是否重名
            for(String name:names.keySet()){
                if(key.equals(name)){
                    throw new ESConfigParseException(this.path + ",{},配置文件" + mappingName + "数据域下面的 数据集中 triggers onInsert 中 indexName 和 collection 名称重复:"+key);
                }
            }
        }

    }

    /**
     * 获取配置文件
     *
     * @return
     */
    protected URL getConfigFile() {
        URL url = DefaultConfigureParser.class.getClassLoader().getResource(path);
        if (null == url) {
            throw new ESConfigParseException(path + "，{} 配置文件不存在");
        } else {
            return url;
        }
    }

    /**
     * 获取全局mapping 默认的分词
     *
     * @param mapping
     * @param key
     * @return
     */
    public List<String> getDefaultTokenizer(Element mapping, String key) {
        String value = mapping.elementTextTrim(key);
        if (StringUtils.isTrimEmpty(value)) {
            return Collections.emptyList();
        } else {
            return Arrays.asList(value.split(","));
        }
    }


    /**
     * 解析mapping 映射方法
     *
     * @param collectionNode 数据集 collection 的 节点 文本
     * @return
     * @throws Exception
     */
    public abstract List<FieldNode> parseMapping(Element collectionNode) throws Exception;

    /**
     * 解析 触发事件的方法
     *
     * @param collectionNode 数据集 collection 的 节点 文本
     * @return
     * @throws Exception
     */
    public abstract List<Trigger> parseEvent(Element collectionNode) throws Exception;


    /**
     * 校验映射类型是否正确
     *
     * @param mappingType
     * @return 返回 映射类型
     */
    protected String checkMappingType(String mappingType) {
        if (!StringUtils.isTrimEmpty(mappingType)) {
            MappingType type = EnumUtil.parseESDataType(mappingType);
            if (type == null) {
                throw new ESConfigParseException(mappingType + "不是可识别的ES类型");
            }
        }
        return mappingType;
    }

    /**
     * 校验keyword是否正确
     *
     * @param keyword
     * @return 返回 keyword
     */
    protected Boolean checkKeyword(String keyword) {
        if (!StringUtils.isTrimEmpty(keyword)) {
            if (keyword.equalsIgnoreCase("false")) {
                return Boolean.FALSE;
            } else if (keyword.equalsIgnoreCase("true")) {
                return Boolean.TRUE;
            } else {
                throw new ESConfigParseException("keyword 是boolean 类型的数据，只能有true，false 值");
            }
        }
        return null;
    }

    /**
     * 校验tokenizer 是否正确，并返回正确的类型
     *
     * @param tokenizer
     */
    protected Tokenizer checkTokenizer(String tokenizer) {
        if (!StringUtils.isTrimEmpty(tokenizer)) {
            Tokenizer token = EnumUtil.parseTokenizer(tokenizer);
            if (token == null) {
                throw new ESConfigParseException(tokenizer + "不是可识别的ES 分词器类型");
            } else {
                return token;
            }
        }
        return null;
    }

    /**
     * 校验 format 是否正确，并返回正确的类型
     *
     * @param format
     */
    protected String checkDateFormat(String format) {
        if (!StringUtils.isTrimEmpty(format)) {
            DateFormat dateFormat = EnumUtil.parseDateFormat(format);
            if (dateFormat == null) {
                throw new ESConfigParseException(format + ",不是可识别的时间格式");
            } else {
                return dateFormat.getCode();
            }
        }
        return null;
    }

    /**
     * 校验even 类型是否正确
     *
     * @param event
     */
    protected Event checkEvent(String event) {
        if (!StringUtils.isTrimEmpty(event)) {
            Event event1 = EnumUtil.parseEvent(event);
            if (event1 == null) {
                throw new ESConfigParseException(event + "不是可识别的操作类型");
            } else {
                return event1;
            }
        }
        return null;
    }

}
