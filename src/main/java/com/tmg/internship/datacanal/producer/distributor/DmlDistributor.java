package com.tmg.internship.datacanal.producer.distributor;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.tmg.internship.datacanal.producer.CanalConfig;
import com.tmg.internship.datacanal.producer.listener.ICanalEventListener;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 只处理增、改、删的数据分发
 * 并且忽略事务事件
 *
 * @author Paul
 * @company 天极云智
 * @date 2018/4/13
 **/
public class DmlDistributor extends AbstractDistributor {

    List<CanalEntry.EntryType> ignoreEntryTypes = Arrays.asList(CanalEntry.EntryType.TRANSACTIONBEGIN, CanalEntry.EntryType.TRANSACTIONEND, CanalEntry.EntryType.HEARTBEAT);

    public DmlDistributor(CanalConnector connector,
                          Map.Entry<String, CanalConfig.Instance> config,
                          List<ICanalEventListener> listeners){
        super(connector,config,listeners);
    }

    @Override
    protected void distributeEvent(Message message) {

        List<CanalEntry.Entry> entries = message.getEntries();
        for (CanalEntry.Entry entry : entries) {
            //ignore the transaction operations
            if (ignoreEntryTypes.stream().anyMatch(t -> entry.getEntryType() == t)) {
                continue;
            }
            CanalEntry.RowChange rowChange;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new CanalClientException("ERROR ## parser of event has an error , data:" + entry.toString(),
                        e);
            }
            //忽略DDL操作
            if (rowChange.hasIsDdl() && rowChange.getIsDdl()) {
                continue;
            }
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                //distribute to listener interfaces
                if (listeners != null) {
                    for (ICanalEventListener listener : listeners) {
                        listener.onEvent(entry.getHeader(), rowChange.getEventType(), rowData);
                    }
                }
            }
        }
    }
}
