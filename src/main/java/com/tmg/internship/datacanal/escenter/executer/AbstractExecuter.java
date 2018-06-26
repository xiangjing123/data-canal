package com.tmg.internship.datacanal.escenter.executer;

import com.tmg.internship.datacanal.escenter.moduls.config.CollectionNode;
import com.tmg.internship.datacanal.escenter.moduls.config.ESConfigure;
import com.tmg.internship.datacanal.escenter.moduls.config.MappingNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 执行器抽象类
 *
 * @author xiangjing
 * @date 2018/5/16
 * @company 天极云智
 */
public abstract class AbstractExecuter implements Executer {

    public static final Logger logger = LoggerFactory.getLogger(AbstractExecuter.class);

    /**
     * 配置文件
     */
    protected ESConfigure configure;

    //数据库名和表名连接符
    public static final String default_connect = "@";

    //数据域
    protected MappingNode mappingNode;

    //数据集
    protected CollectionNode collectionNode;

    public AbstractExecuter(ESConfigure configure) {
        this.configure = configure;
    }

    /**
     * 获取表名
     *
     * @param index
     * @return
     */
    protected String getCollectionName(String index) {
        return index.split(default_connect)[1];
    }

    /**
     * 获取数据域名称（对应数据库的库名）
     *
     * @param index
     * @return
     */
    protected String getMappingName(String index) {
        return index.split(default_connect)[0];
    }

    /**
     * 解析MappingNode 节点
     *
     * @return
     */
    protected void parseMappingConfig(String schemaName) {
        this.mappingNode = configure.getMappingNode(schemaName);
    }

    /**
     * 解析 collectionNode 节点的配置
     *
     * @param index
     */
    protected void parseCollectionConfig(String index) {
        parseMappingConfig(getMappingName(index));//解析mappingNode
        if (this.mappingNode != null) {
            this.collectionNode = this.mappingNode.getCollectionNode(getCollectionName(index));
        }
    }
}
