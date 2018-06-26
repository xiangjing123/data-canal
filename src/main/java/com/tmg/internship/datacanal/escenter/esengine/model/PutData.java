package com.tmg.internship.datacanal.escenter.esengine.model;

import java.util.Map;

/**
 * @author chensl [cookchensl@gmail.com]
 * @date 2018/5/15 14:14
 * @description
 */
public class PutData extends BaseData{
    /**
     * document source
     */
    private Map<String, Object> jsonMap;



    private String versionType;

    /**
     *Operation type provided as a String: can be create or update (default)
     */
    private String opType;

    /**
     * The name of the ingest pipeline to be executed before indexing the document
     * 索引文档之前要执行的摄取管道的名称
     */
    private String pipeline;


    public String getVersionType() {
        return versionType;
    }

    public void setVersionType(String versionType) {
        this.versionType = versionType;
    }

    public String getOpType() {
        return opType;
    }

    public void setOpType(String opType) {
        this.opType = opType;
    }

    public String getPipeline() {
        return pipeline;
    }

    public void setPipeline(String pipeline) {
        this.pipeline = pipeline;
    }
    public Map<String, Object> getJsonMap() {
        return jsonMap;
    }

    public void setJsonMap(Map<String, Object> jsonMap) {
        this.jsonMap = jsonMap;
    }
}
