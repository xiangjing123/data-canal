package com.tmg.internship.datacanal.escenter.esengine.model;

import java.io.Serializable;

/**
 * @author chensl [cookchensl@gmail.com]
 * @date 2018/5/15 11:18
 * @description
 */
public class BaseIndex implements Serializable{
    private static final long serialVersionUID = -5482372309746866763L;

    /**
     * 索引名
     */
    private String index;

    /**
     * Timeout to wait for the all the nodes to acknowledge the index creation as a String
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
     * Timeout to connect to the master node
     * 2m   2分钟
     * 2s   2秒
     * 2h   2小时
     * 2d   2天
     * 2ms  2毫秒
     * 2micros 2微秒
     * 2nanos 2纳秒
     */
    private String masterNodeTimeout;

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

    public String getTimeOut() {
        return timeOut;
    }

    public void setTimeOut(String timeOut) {
        this.timeOut = timeOut;
    }

    public String getExecution() {
        return execution;
    }

    public void setExecution(String execution) {
        this.execution = execution;
    }

    public String getMasterNodeTimeout() {
        return masterNodeTimeout;
    }

    public void setMasterNodeTimeout(String masterNodeTimeout) {
        this.masterNodeTimeout = masterNodeTimeout;
    }
}
