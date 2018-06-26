package com.tmg.internship.datacanal.escenter.moduls.config;

import com.tmg.internship.datacanal.escenter.common.BaseClass;
import com.tmg.utils.StringUtils;

import java.util.List;

/**
 * ES 配置文件
 *
 * @author xiangjing
 * @date 2018/5/7
 * @company 天极云智
 */
public class ESConfigure  extends BaseClass {

    //默认主分片数
    private Integer numberOfShards;

    //默认副分片数
    private Integer numberOfReplicas;

    // 用于定义数据域，如：数据库名，日志类型名(runtime_log,business_log)等
    private List<MappingNode> mappings;

    /**
     * 获取主分片数
     * @return
     */
    public Integer getNumberOfShards() {
        return numberOfShards;
    }

    /**
     * 设置主分片数
     * @param numberOfShards
     */
    public void setNumberOfShards(Integer numberOfShards) {
        this.numberOfShards = numberOfShards;
    }

    /**
     * 获取副分片数
     * @return
     */
    public Integer getNumberOfReplicas() {
        return numberOfReplicas;
    }

    /**
     * 设置副分片数
     * @param numberOfReplicas
     */
    public void setNumberOfReplicas(Integer numberOfReplicas) {
        this.numberOfReplicas = numberOfReplicas;
    }

    /**
     * 获取数据域
     * @return 返回所有的数据域的配置
     */
    public List<MappingNode> getMappings() {
        return mappings;
    }

    /**
     * 设置数据域
     * @param mappings
     */
    public void setMappings(List<MappingNode> mappings) {
        this.mappings = mappings;
    }

    /**
     * 根据数据域的名称获取ES配置里面的数据域配置
     * @param schemaName 数据域的名称
     * @return
     */
    public MappingNode getMappingNode(String schemaName){
        if(StringUtils.isEmpty(schemaName)){
            return null;
        }

        if(null == this.mappings || this. mappings.size() == 0){
            return null;
        }
        for(MappingNode mapping:mappings){
            if(mapping.getSchemaName().equals(schemaName)){
                return mapping;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "ESConfigure{" +
                "numberOfShards=" + numberOfShards +
                ", numberOfReplicas=" + numberOfReplicas +
                ", mappings=" + mappings +
                '}';
    }
}
