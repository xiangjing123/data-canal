package com.tmg.internship.datacanal.escenter.esengine.model;

import java.io.Serializable;

/**
 * @author chensl [cookchensl@gmail.com]
 * @date 2018/5/15 14:04
 * @description
 */
public class BaseData implements Serializable {
    private static final long serialVersionUID = -2180114363491724140L;

    /**
     * 索引
     */
    private String index;

    /**
     * 类型
     */
    private String type;

    /**
     * 文档id
     */
    private String documentId;

    /**
     * 路由
     */
    private String routing;

    /**
     * 父文档
     */
    private String parent;

    /**
     * Timeout to wait for primary shard to become available
     * 等待主分片变为可用的超时
     * 2m   2分钟
     * 2s   2秒
     * 2h   2小时
     * 2d   2天
     * 2ms  2毫秒
     * 2micros 2微秒
     * 2nanos 2纳秒
     */
    private String timeOut;

    /**
     * 刷新策略
     * 可选：wait_for
     * true   实时刷新
     * false
     */
    private String refreshPolicy;

    private long version;


    /**
     * 默认同步执行
     * 可选： asynchronous 异步执行   synchronous同步执行
     */
    private String execution="synchronous";


    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDocumentId() {
        return documentId;
    }

    public void setDocumentId(String documentId) {
        this.documentId = documentId;
    }

    public String getRouting() {
        return routing;
    }

    public void setRouting(String routing) {
        this.routing = routing;
    }

    public String getParent() {
        return parent;
    }

    public void setParent(String parent) {
        this.parent = parent;
    }

    public String getTimeOut() {
        return timeOut;
    }

    public void setTimeOut(String timeOut) {
        this.timeOut = timeOut;
    }

    public String getRefreshPolicy() {
        return refreshPolicy;
    }

    public void setRefreshPolicy(String refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public String getExecution() {
        return execution;
    }

    public void setExecution(String execution) {
        this.execution = execution;
    }
}
