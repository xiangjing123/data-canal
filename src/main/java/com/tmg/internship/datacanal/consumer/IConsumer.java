package com.tmg.internship.datacanal.consumer;

/**
 * canal事件消费者接口
 *
 * @author Paul
 * @company 天极云智
 * @date 2018/4/17
 **/
public interface IConsumer {

    /**
     * 启动消费者开始消费事件消息
     * 可以有多个实例订阅不同的tag，同时消费消息
     */
    void start() throws Exception;

    /**
     * 停止消费
     */
    void stop();

    /**
     * 获取运行状态
     * @return yes or no
     */
    boolean isRunning();

}
