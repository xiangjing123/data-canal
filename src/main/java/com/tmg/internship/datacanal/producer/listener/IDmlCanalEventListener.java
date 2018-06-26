package com.tmg.internship.datacanal.producer.listener;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.Objects;

/**
 * 只关注增、改、删事件的监听器接口
 *
 * @author Paul
 * @company 天极云智
 * @date 2018/4/13
 **/
public interface IDmlCanalEventListener extends ICanalEventListener {

    @Override
    default void onEvent(CanalEntry.Header header,CanalEntry.EventType eventType, CanalEntry.RowData rowData) {
        Objects.requireNonNull(eventType);
        switch (eventType) {
            case INSERT:
                onInsert(header.getSchemaName(), header.getTableName(), rowData);
                break;
            case UPDATE:
                onUpdate(header.getSchemaName(), header.getTableName(), rowData);
                break;
            case DELETE:
                onDelete(header.getSchemaName(), header.getTableName(), rowData);
                break;
            default:
                break;
        }
    }

    /**
     * 新增数据时触发
     *
     * @param schemaName 数据库名
     * @param tableName 表名
     * @param rowData 受影响的行数据
     */
    void onInsert(String schemaName,String tableName,CanalEntry.RowData rowData);

    /**
     * 修改数据时触发
     *
     * @param schemaName 数据库名
     * @param tableName 表名
     * @param rowData 受影响的行数据
     */
    void onUpdate(String schemaName,String tableName,CanalEntry.RowData rowData);

    /**
     * 删除数据时触发
     *
     * @param schemaName 数据库名
     * @param tableName 表名
     * @param rowData 受影响的行数据
     */
    void onDelete(String schemaName,String tableName,CanalEntry.RowData rowData);
}
