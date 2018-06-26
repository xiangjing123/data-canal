package com.tmg.internship.datacanal.producer.client;

import com.tmg.internship.datacanal.producer.listener.ICanalEventListener;

/**
 * canal客户端基础接口，只关注启动、停止和运行状态查询
 *
 * @author Paul
 * @company 天极云智
 * @date 2018/4/13
 **/
public interface ICanalClient {

    /**
     * open the canal client
     * to get the config and connect to the canal server (1 : 1 or 1 : n)
     * and then  transfer the event to the special listener
     * */
    void start();

    /**
     * stop the client
     */
    void stop();

    /**
     * is running
     * @return yes or no
     */
    boolean isRunning();

    /**
     * add listener for firing canal event
     * @param listener
     */
    void addListener(ICanalEventListener listener);
}
