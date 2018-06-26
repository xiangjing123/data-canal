package com.tmg.internship.datacanal.escenter.moduls.mapping;

import java.util.Map;

/**
 * ES 索引变更后的数据
 *
 * @author xiangjing
 * @date 2018/5/16
 * @company 天极云智
 */
public class IndexChange {

    /**
     * 映射和分词器变更类型
     */
    private ChangeType changeType;

    /**
     * 查看分片是否改变
     */
    private Boolean shardChange;

    /**
     * 需要添加的mapping 和setting
     */
    private Map<String,Map<String,Object>> indexChange;

    /**
     * 索引的改变类型
     */
    public enum ChangeType{
        ADD(6),//既要添加mapping 又要添加setting
        MAPPING_ADD(5),//添加映射
        SETTING_ADD(4),//添加配置
        NOT_EXIST(3),//映射不存在
        UPDATE(2),//映射已经修改
        NO_CHANGE(1);//映射没变化

        private int code;

        ChangeType(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public void setCode(int code) {
            this.code = code;
        }
    }

    public ChangeType getChangeType() {
        return changeType;
    }

    public void setChangeType(ChangeType changeType) {
        this.changeType = changeType;
    }

    public Boolean getShardChange() {
        return shardChange;
    }

    public void setShardChange(Boolean shardChange) {
        this.shardChange = shardChange;
    }

    public Map<String, Map<String, Object>> getIndexChange() {
        return indexChange;
    }

    public void setIndexChange(Map<String, Map<String, Object>> indexChange) {
        this.indexChange = indexChange;
    }


    @Override
    public String toString() {
        return "IndexChange{" +
                "changeType=" + changeType +
                ", shardChange=" + shardChange +
                ", indexChange=" + indexChange +
                '}';
    }
}
