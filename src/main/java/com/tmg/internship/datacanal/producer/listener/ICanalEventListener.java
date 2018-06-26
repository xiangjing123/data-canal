package com.tmg.internship.datacanal.producer.listener;

import com.alibaba.otter.canal.protocol.CanalEntry;

/**
 * canal事件监听器，用于传送数据库数据变更事件及数据
 *
 * @author Paul
 * @company 天极云智
 * @date 2018/4/13
 **/
public interface ICanalEventListener {

    /**
     * 数据变更事件触发时调用
     *
     * @param header 事件元信息
     * @param eventType 事件类型
     * @param rowData 行数据
     */
    void onEvent(CanalEntry.Header header, CanalEntry.EventType eventType, CanalEntry.RowData rowData);
}
