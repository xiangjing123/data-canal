package com.tmg.internship.datacanal.escenter.esengine.model;

import java.util.Map;

/**
 * @author chensl [cookchensl@gmail.com]
 * @date 2018/5/16 20:07
 * @description
 */
public class UpdateData extends BaseData{

    private Map<String,Object> docJsonMap;

    /**
     *How many times to retry the update operation if the document to update has been changed by another operation between the get and indexing phases of the update operation
     * 如果要更新的文档已被更新操作的获取和索引阶段之间的另一操作更改，则重试更新操作的次数
     */
    private int retryOnConflict;

    /**
     * Enable source retrieval, disabled by default
     * 启用源检索，默认情况下禁用
     */
    private boolean fetchSource;

    /**
     * noop检测
     */
    private boolean detectNoop;

    /**
     * 表明无论文档是否存在，脚本都必须运行，即如果脚本尚不存在，则脚本负责创建文档
     */
    private boolean scriptedUpsert;

    /**
     * 如果不存在，则表明部分文档必须用作upsert文档。
     */
    private boolean docAsUpsert;

    /**
     * 设置在继续更新操作之前必须激活的分片副本的数量
     */
    private int waitForActiveShards;

    public Map<String, Object> getDocJsonMap() {
        return docJsonMap;
    }

    public void setDocJsonMap(Map<String, Object> docJsonMap) {
        this.docJsonMap = docJsonMap;
    }

    public int getRetryOnConflict() {
        return retryOnConflict;
    }

    public void setRetryOnConflict(int retryOnConflict) {
        this.retryOnConflict = retryOnConflict;
    }

    public boolean isFetchSource() {
        return fetchSource;
    }

    public void setFetchSource(boolean fetchSource) {
        this.fetchSource = fetchSource;
    }

    public boolean isDetectNoop() {
        return detectNoop;
    }

    public void setDetectNoop(boolean detectNoop) {
        this.detectNoop = detectNoop;
    }

    public boolean isScriptedUpsert() {
        return scriptedUpsert;
    }

    public void setScriptedUpsert(boolean scriptedUpsert) {
        this.scriptedUpsert = scriptedUpsert;
    }

    public boolean isDocAsUpsert() {
        return docAsUpsert;
    }

    public void setDocAsUpsert(boolean docAsUpsert) {
        this.docAsUpsert = docAsUpsert;
    }

    public int getWaitForActiveShards() {
        return waitForActiveShards;
    }

    public void setWaitForActiveShards(int waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
    }
}
