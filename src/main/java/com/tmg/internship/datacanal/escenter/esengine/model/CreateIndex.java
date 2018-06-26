package com.tmg.internship.datacanal.escenter.esengine.model;

import java.util.Map;

/**
 * @author chensl [cookchensl@gmail.com]
 * @date 2018/5/15 9:52
 * @description 创建索引实体类
 */
public class CreateIndex extends BaseIndex{
    /**
     * 类型名
     */
    private String type;

    /**
     * settings
     */
    private  Map<String,Object> settings;


    /**
     * The mapping for this type, provided as a JSON string
     */
    private Map<String,Object> mappings;

    /**
     * 别名
     */
    private String alias;


    /**
     * The number of active shard copies to wait for before the create index API returns a response, as an int
     */
    private int waitForActiveShards;


    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getMappings() {
        return mappings;
    }

    public void setMappings(Map<String, Object> mappings) {
        this.mappings = mappings;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public int getWaitForActiveShards() {
        return waitForActiveShards;
    }

    public void setWaitForActiveShards(int waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
    }

    public Map<String, Object> getSettings() {
        return settings;
    }

    public void setSettings(Map<String, Object> settings) {
        this.settings = settings;
    }
}
