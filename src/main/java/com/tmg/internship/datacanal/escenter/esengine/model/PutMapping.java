package com.tmg.internship.datacanal.escenter.esengine.model;

import java.io.Serializable;
import java.util.Map;

/**
 * @author chensl [cookchensl@gmail.com]
 * @date 2018/5/16 20:42
 * @description
 */
public class PutMapping implements Serializable{
    private static final long serialVersionUID = -5071473791281748559L;

    private String index;

    private String type;

    private Map<String,Object> source;

    private String timeOut;

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

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getSource() {
        return source;
    }

    public void setSource(Map<String, Object> source) {
        this.source = source;
    }

    public String getTimeOut() {
        return timeOut;
    }

    public void setTimeOut(String timeOut) {
        this.timeOut = timeOut;
    }

    public String getMasterNodeTimeout() {
        return masterNodeTimeout;
    }

    public void setMasterNodeTimeout(String masterNodeTimeout) {
        this.masterNodeTimeout = masterNodeTimeout;
    }

    public String getExecution() {
        return execution;
    }

    public void setExecution(String execution) {
        this.execution = execution;
    }
}
