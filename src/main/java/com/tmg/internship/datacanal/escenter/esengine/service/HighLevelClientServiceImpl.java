package com.tmg.internship.datacanal.escenter.esengine.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.tmg.internship.datacanal.escenter.esengine.model.*;
import com.tmg.utils.JsonUtils;
import com.tmg.utils.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.*;

/**
 * @author chensl [cookchensl@gmail.com]
 * @date 2018/5/11 11:24
 * @description
 */
@Service
public class HighLevelClientServiceImpl implements HighLevelClientService {

    private final static Logger logger = LoggerFactory.getLogger(HighLevelClientServiceImpl.class);

    private static final String ASYNCHRONOUS_EXECUTION = "asynchronous";
    private static final String CHARSET = "utf-8";
    private static final String PUT = "PUT";
    private static final String POST = "POST";
    private static final String GET = "GET";
    private static final String HEAD = "HEAD";
    private static final String DELETE = "DELETE";
    private static final String DEFAULT_TYPE = "_doc";


    @Autowired
    private RestHighLevelClient restHighLevelClient;

    private static RestHighLevelClient client;
    private static RestClient restClient;


   @PostConstruct
    public void init() {
        client = this.restHighLevelClient;
        restClient = this.restHighLevelClient.getLowLevelClient();
    }

    @PreDestroy
    public void destroy() throws IOException {
        if (client != null) {
            client.close();
        }
        if (restClient != null) {
            restClient.close();
        }
    }


    @Override
    public boolean createIndex(String index) throws IOException {
        CreateIndex createIndex = new CreateIndex();
        createIndex.setIndex(index);
        return createIndex(createIndex);
    }

    @Override
    public boolean createIndex(String index, Map<String, Object> setting, String type, Map<String, Object> mapping, String alias) throws IOException {
        CreateIndex createIndex = new CreateIndex();
        createIndex.setIndex(index);
        if (StringUtils.isTrimEmpty(type)) {
            createIndex.setType(DEFAULT_TYPE);
        } else {
            createIndex.setType(type);
        }
        createIndex.setSettings(setting);
        createIndex.setMappings(mapping);
        createIndex.setAlias(alias);
        return createIndex(createIndex);
    }

    @Override
    public boolean createIndex(String index, Map<String, Object> setting) throws IOException {
        CreateIndex createIndex = new CreateIndex();
        createIndex.setIndex(index);
        createIndex.setSettings(setting);
        return createIndex(createIndex);
    }

    @Override
    public boolean createIndex(String index, Map<String, Object> setting, String alias) throws IOException {
        CreateIndex createIndex = new CreateIndex();
        createIndex.setIndex(index);
        createIndex.setSettings(setting);
        createIndex.setAlias(alias);
        return createIndex(createIndex);
    }

    @Override
    public boolean createIndex(String index, Map<String, Object> mapping, String type, String alias) throws IOException {
        CreateIndex createIndex = new CreateIndex();
        createIndex.setIndex(index);
        createIndex.setMappings(mapping);
        if (StringUtils.isTrimEmpty(type)) {
            createIndex.setType(DEFAULT_TYPE);
        } else {
            createIndex.setType(type);
        }
        createIndex.setAlias(alias);
        return createIndex(createIndex);
    }

    @Override
    public boolean deleteIndex(String index) throws IOException {
        BaseIndex baseIndex = new BaseIndex();
        baseIndex.setIndex(index);
        return deleteIndex(baseIndex);
    }

    @Override
    public boolean openIndex(String index) throws IOException {
        BaseIndex baseIndex = new BaseIndex();
        baseIndex.setIndex(index);
        return openIndex(baseIndex);
    }

    @Override
    public boolean closeIndex(String index) throws IOException {
        BaseIndex baseIndex = new BaseIndex();
        baseIndex.setIndex(index);
        return closeIndex(baseIndex);
    }

    @Override
    public boolean createIndex(CreateIndex createIndex) throws IOException {
        if (createIndex != null && !StringUtils.isTrimEmpty(createIndex.getIndex())) {
            CreateIndexRequest request = new CreateIndexRequest(createIndex.getIndex().toLowerCase());
            //Settings for this index
            if (createIndex.getSettings() != null) {
                request.settings(createIndex.getSettings());
            }
            //Mappings for this index
            if (!StringUtils.isTrimEmpty(createIndex.getType()) && createIndex.getMappings() != null) {
                request.mapping(createIndex.getType(), createIndex.getMappings());
            }
            // set aliases
            if (!StringUtils.isTrimEmpty(createIndex.getAlias())) {
                request.alias(new Alias(createIndex.getAlias()));
            }
            //Timeout to wait for the all the nodes to acknowledge the index creation
            if (!StringUtils.isTrimEmpty(createIndex.getTimeOut())) {
                request.timeout(createIndex.getTimeOut());
            }
            //Timeout to connect to the master node
            if (!StringUtils.isTrimEmpty(createIndex.getMasterNodeTimeout())) {
                request.masterNodeTimeout(createIndex.getMasterNodeTimeout());
            }
            //The number of active shard copies to wait for before the create index API returns a response
            if (createIndex.getWaitForActiveShards() > 0) {
                request.waitForActiveShards(createIndex.getWaitForActiveShards());
            }
            //同步执行
            if (!ASYNCHRONOUS_EXECUTION.equalsIgnoreCase(createIndex.getExecution())) {
                CreateIndexResponse createIndexResponse = client.indices().create(request);
                return createIndexResponse.isAcknowledged();
            } else {
                //异步执行
                ActionListener<CreateIndexResponse> listener = new ActionListener<CreateIndexResponse>() {
                    @Override
                    public void onResponse(CreateIndexResponse createIndexResponse) {
                        logger.info("create index success:" + createIndexResponse.toString());
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error(e.getMessage(), e);
                        e.printStackTrace();
                    }
                };
                client.indices().createAsync(request, listener);
                return true;
            }
        } else {
            logger.info("create index fail, index is null");
            throw new IllegalStateException("create index fail, index is null");
        }
    }

    @Override
    public boolean deleteIndex(BaseIndex baseIndex) throws IOException {
        if (baseIndex != null && !StringUtils.isTrimEmpty(baseIndex.getIndex())) {
            DeleteIndexRequest request = new DeleteIndexRequest(baseIndex.getIndex().toLowerCase());
            if (!StringUtils.isTrimEmpty(baseIndex.getTimeOut())) {
                request.timeout(baseIndex.getTimeOut());
            }
            if (!StringUtils.isTrimEmpty(baseIndex.getMasterNodeTimeout())) {
                request.masterNodeTimeout(baseIndex.getMasterNodeTimeout());
            }
            if (!ASYNCHRONOUS_EXECUTION.equalsIgnoreCase(baseIndex.getExecution())) {
                try {
                    DeleteIndexResponse deleteIndexResponse = client.indices().delete(request);
                    return deleteIndexResponse.isAcknowledged();
                } catch (ElasticsearchException exception) {
                    if (exception.status() == RestStatus.NOT_FOUND) {
                        logger.error("the index:" + baseIndex.getIndex() + " to be deleted was not found");
                    }
                    throw exception;
                }
            } else {
                ActionListener<DeleteIndexResponse> listener = new ActionListener<DeleteIndexResponse>() {
                    @Override
                    public void onResponse(DeleteIndexResponse deleteIndexResponse) {
                        logger.info("create index success:" + baseIndex.toString());
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error(e.getMessage(), e);
                        e.printStackTrace();
                    }
                };
                client.indices().deleteAsync(request, listener);
                return true;
            }
        } else {
            logger.info("delete index fail , index can not be null");
            return false;
        }
    }

    @Override
    public boolean openIndex(BaseIndex baseIndex) throws IOException {
        if (baseIndex != null && !StringUtils.isTrimEmpty(baseIndex.getIndex())) {
            OpenIndexRequest request = new OpenIndexRequest(baseIndex.getIndex().toLowerCase());
            OpenIndexResponse openIndexResponse = client.indices().open(request);
            return openIndexResponse.isAcknowledged();
        } else {
            logger.info("open index fail , index can not be null");
            return false;
        }
    }

    @Override
    public boolean closeIndex(BaseIndex baseIndex) throws IOException {
        if (baseIndex != null && !StringUtils.isTrimEmpty(baseIndex.getIndex())) {
            CloseIndexRequest request = new CloseIndexRequest(baseIndex.getIndex().toLowerCase());
            CloseIndexResponse closeIndexResponse = client.indices().close(request);
            return closeIndexResponse.isAcknowledged();

        } else {
            logger.info("close index fail , index can not be null");
            return false;
        }
    }


    @Override
    public boolean putDocument(PutData data) throws IOException {
        if (StringUtils.isTrimEmpty(data.getIndex())) {
            logger.info("index can not be null");
            throw new IllegalStateException("put data fail , index can not be null");
        }
        if (StringUtils.isTrimEmpty(data.getType())) {
            logger.info("type can not be null");
            throw new IllegalStateException("put data fail , type can not be null");
        }
        if (data.getJsonMap() == null) {
            logger.info("The document source can not be null");
            throw new IllegalStateException("put data fail , the document source can not be null");
        }
        IndexRequest request = null;
        if (StringUtils.isTrimEmpty(data.getDocumentId())) {
            request = new IndexRequest(data.getIndex(), data.getType());
        } else {
            request = new IndexRequest(data.getIndex(), data.getType(), data.getDocumentId());
        }
        request.source(data.getJsonMap());

        if (!StringUtils.isTrimEmpty(data.getRouting())) {
            request.routing(data.getRouting());
        }
        if (!StringUtils.isTrimEmpty(data.getParent())) {
            request.parent(data.getParent());
        }
        if (!StringUtils.isTrimEmpty(data.getTimeOut())) {
            request.timeout(data.getTimeOut());
        }
        if (!StringUtils.isTrimEmpty(data.getRefreshPolicy())) {
            request.setRefreshPolicy(data.getRefreshPolicy());
        }
        if (data.getVersion() > 0) {
            request.version(data.getVersion());
        }
        if (!StringUtils.isTrimEmpty(data.getOpType())) {
            request.opType(data.getOpType());
        }
        if (!StringUtils.isTrimEmpty(data.getPipeline())) {
            request.setPipeline(data.getPipeline());
        }
        if (!ASYNCHRONOUS_EXECUTION.equalsIgnoreCase(data.getExecution())) {
            try {
                IndexResponse indexResponse = client.index(request);
                if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                    logger.info("create success");
                    return true;

                } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                    logger.info("update success");
                    return true;
                }
            } catch (ElasticsearchException e) {
                if (e.status() == RestStatus.CONFLICT) {
                    logger.error("there is a version conflict");
                }
                throw e;
            }
            return false;
        } else {
            ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {

                }

                @Override
                public void onFailure(Exception e) {

                }
            };
            client.indexAsync(request, listener);
            return true;
        }
    }

    @Override
    public boolean isExists(String indexName, String typeName, String fileId) throws IOException {
        GetRequest getRequest =  new GetRequest(indexName, typeName, fileId);
        /**
         *  turning off fetching _source and any stored fields so the request is slightly lighter
         */
        //禁止获取_source
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        //禁止获取存储的字段
        getRequest.storedFields("_none_");
        return client.exists(getRequest);

    }

    @Override
    public boolean deleteDocument(BaseData baseData) throws IOException {
        if (baseData != null && !StringUtils.isTrimEmpty(baseData.getIndex())) {
            DeleteRequest deleteRequest = null;
            if (!StringUtils.isTrimEmpty(baseData.getType()) && !StringUtils.isTrimEmpty(baseData.getDocumentId())) {
                deleteRequest = new DeleteRequest(baseData.getIndex(), baseData.getType(), baseData.getDocumentId());
            } else {
                deleteRequest = new DeleteRequest(baseData.getIndex());
            }
            if (!StringUtils.isTrimEmpty(baseData.getRouting())) {
                deleteRequest.routing(baseData.getRouting());
            }
            if (!StringUtils.isTrimEmpty(baseData.getParent())) {
                deleteRequest.parent(baseData.getParent());
            }
            if (!StringUtils.isTrimEmpty(baseData.getTimeOut())) {
                deleteRequest.timeout(baseData.getTimeOut());
            }
            if (!StringUtils.isTrimEmpty(baseData.getRefreshPolicy())) {
                deleteRequest.setRefreshPolicy(baseData.getRefreshPolicy());
            }
            if (baseData.getVersion() > 0) {
                deleteRequest.version(baseData.getVersion());
            }
            if (!ASYNCHRONOUS_EXECUTION.equalsIgnoreCase(baseData.getExecution())) {
                try {
                    DeleteResponse deleteResponse = client.delete(deleteRequest);
                    if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                        throw new IllegalStateException(" the document was not found");
                    }
                    if (deleteResponse.getShardInfo().getTotal() != deleteResponse.getShardInfo().getSuccessful()) {
                        return false;
                    } else {
                        return true;
                    }
                } catch (ElasticsearchException exception) {
                    if (exception.status() == RestStatus.CONFLICT) {
                        logger.error("there is a version conflict");
                        exception.printStackTrace();
                    }
                }
                return false;
            } else {
                ActionListener<DeleteResponse> listener = new ActionListener<DeleteResponse>() {
                    @Override
                    public void onResponse(DeleteResponse deleteResponse) {
                        logger.info("delete success");
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error(e.getMessage(), e);
                        e.printStackTrace();
                    }
                };
                client.deleteAsync(deleteRequest, listener);
                return true;
            }
        } else {
            logger.info("delete  fail, index is null");
            throw new IllegalStateException("delete  fail, index is null");
        }
    }

    @Override
    public void updateDocument(UpdateData updateData) throws IOException {
        if (updateData != null && !StringUtils.isTrimEmpty(updateData.getIndex()) && !StringUtils.isTrimEmpty(updateData.getType()) && !StringUtils.isTrimEmpty(updateData.getDocumentId())) {
            UpdateRequest request = new UpdateRequest(updateData.getIndex(), updateData.getType(), updateData.getDocumentId());
            request.doc(updateData.getDocJsonMap());
            if (!StringUtils.isTrimEmpty(updateData.getRouting())) {
                request.routing(updateData.getRouting());
            }
            if (!StringUtils.isTrimEmpty(updateData.getParent())) {
                request.parent(updateData.getParent());
            }
            if (!StringUtils.isTrimEmpty(updateData.getTimeOut())) {
                request.timeout(updateData.getTimeOut());
            }
            if (!StringUtils.isTrimEmpty(updateData.getRefreshPolicy())) {
                request.setRefreshPolicy(updateData.getRefreshPolicy());
            }
            if (updateData.getRetryOnConflict() > 0) {
                request.retryOnConflict(updateData.getRetryOnConflict());
            }
            if (updateData.getWaitForActiveShards() > 0) {
                request.waitForActiveShards(updateData.getWaitForActiveShards());
            }
            if (!ASYNCHRONOUS_EXECUTION.equalsIgnoreCase(updateData.getExecution())) {
                try {
                    UpdateResponse updateResponse = client.update(request);
                    ReplicationResponse.ShardInfo shardInfo = updateResponse.getShardInfo();
                    if (shardInfo.getFailed() > 0) {
                        StringBuilder sb = new StringBuilder();
                        for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                            sb.append(failure.reason());
                        }
                        logger.error(sb.toString());
                        throw new IllegalStateException(sb.toString());
                    } else {
                        logger.info("update data success from " + updateData.toString());
                    }
                } catch (ElasticsearchException e) {
                    if (e.status() == RestStatus.NOT_FOUND) {
                        logger.error(e.getMessage(), e);
                        e.printStackTrace();
                    }
                    if (e.status() == RestStatus.CONFLICT) {
                        logger.error(e.getMessage(), e);
                        e.printStackTrace();
                    }
                }
            } else {
                ActionListener<UpdateResponse> listener = new ActionListener<UpdateResponse>() {
                    @Override
                    public void onResponse(UpdateResponse updateResponse) {
                        logger.info("update data success from " + updateData.toString());
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                };
                client.updateAsync(request, listener);
            }
        } else {
            logger.info("update data params error");
            throw new IllegalStateException("update data params error");
        }
    }

    @Override
    public boolean putMapping(PutMapping putMapping) throws IOException {
        if (putMapping != null && !StringUtils.isTrimEmpty(putMapping.getIndex()) && putMapping.getSource() != null && !StringUtils.isTrimEmpty(putMapping.getType())) {
            String source = JsonUtils.toJSONString(putMapping.getSource());
            HttpEntity entity = new NStringEntity(source, ContentType.APPLICATION_JSON);
            Response response = restClient.performRequest(PUT, "/" + putMapping.getIndex() + "/_mapping/" + putMapping.getType(), Collections.singletonMap("pretty", "true"), entity);

            JSONObject jsonObj = JSON.parseObject(EntityUtils.toString(response.getEntity()));
            return jsonObj.getBooleanValue("acknowledged");

        } else {
            throw new IllegalStateException("params error,index & type & source can not be null  ");
        }
    }

    @Override
    public boolean putMapping(String index, String type, Map<String, Object> source) throws IOException {
        PutMapping putMapping = new PutMapping();
        putMapping.setIndex(index);
        putMapping.setType(type);
        putMapping.setSource(source);
        return putMapping(putMapping);
    }

    @Override
    public boolean updateSettings(String index, Map<String, Object> settings) throws IOException {
        if (!StringUtils.isTrimEmpty(index) && settings != null) {
            String source = JsonUtils.toJSONString(settings);
            HttpEntity entity = new NStringEntity(source, ContentType.APPLICATION_JSON);
            Response response = restClient.performRequest(PUT, "/" + index + "/_settings", Collections.singletonMap("pretty", "true"), entity);

            JSONObject jsonObj = JSON.parseObject(EntityUtils.toString(response.getEntity()));
            return jsonObj.getBooleanValue("acknowledged");
        } else {
            throw new IllegalStateException("params error,index & settings can not be null  ");
        }
    }

    @Override
    public boolean indexOrDocumentIsExists(String index, String type, String documentId) throws IOException {
        int code = 200;
        String endpoint = "/" + index + "/" + type + "/" + documentId;
        if (StringUtils.isBlank(type) || StringUtils.isBlank(documentId)) {
            endpoint = "/" + index;
        }
        //200存在，404不存在
        Response response = restClient.performRequest(HEAD, endpoint);
        code = response.getStatusLine().getStatusCode();

        return code == 200 ? true : false;
    }

    @Override
    public boolean delete(String index, String type, String id) throws IOException {
        String endpoint = "/" + index + "/" + type + "/" + id;
        if (StringUtils.isBlank(id)) {
            endpoint = "/" + index;
        } else if (StringUtils.isBlank(type)) {
            endpoint = "/" + index;
        }
        Response response = restClient.performRequest(DELETE, endpoint);
        JSONObject jsonObj = JSON.parseObject(EntityUtils.toString(response.getEntity()));
        if (StringUtils.isBlank(id) || StringUtils.isBlank(type)) {
            return jsonObj.getBooleanValue("acknowledged");
        } else {
            return "deleted".equalsIgnoreCase(jsonObj.getString("result")) ? true : false ;
        }

    }

    @Override
    public void bulkData(List<PutData> putDataList) throws IOException {
        BulkRequest request = new BulkRequest();
        for (PutData putData : putDataList) {
            request.add(new IndexRequest(putData.getIndex(), putData.getType(), putData.getDocumentId()).source(putData.getJsonMap()));
        }
        BulkResponse bulkResponse = client.bulk(request);
        StringBuilder stringBuilder = new StringBuilder();
        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            if (bulkItemResponse.isFailed()) {
                BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                stringBuilder.append(failure.getCause());
            }
        }
        if (stringBuilder.length() > 0) {
            logger.error("Failed to execute bulk", stringBuilder);
            throw new IllegalStateException(stringBuilder.toString());
        }

    }


    @Override
    public boolean putDocumentWithId(String index, String type, String id, Map<String, Object> data) throws IOException {
        PutData putData = new PutData();
        putData.setIndex(index);
        if (StringUtils.isTrimEmpty(type)) {
            putData.setType(DEFAULT_TYPE);
        } else {
            putData.setType(type);
        }
        putData.setDocumentId(id);
        putData.setJsonMap(data);
        return putDocument(putData);
    }

    @Override
    public boolean putDocumentWithVersion(String index, String type, String id, long version, Map<String, Object> data) throws IOException {
        PutData putData = new PutData();
        putData.setIndex(index);
        if (StringUtils.isTrimEmpty(type)) {
            putData.setType(DEFAULT_TYPE);
        } else {
            putData.setType(type);
        }
        putData.setDocumentId(id);
        putData.setJsonMap(data);
        putData.setVersion(version);
        return putDocument(putData);
    }

    @Override
    public boolean postDocumentWithRouting(String index, String type, String routing, Map<String, Object> data) throws IOException {
        PutData putData = new PutData();
        putData.setIndex(index);
        if (StringUtils.isTrimEmpty(type)) {
            putData.setType(DEFAULT_TYPE);
        } else {
            putData.setType(type);
        }
        putData.setJsonMap(data);
        putData.setRouting(routing);
        return putDocument(putData);
    }

    @Override
    public boolean postDocument(String index, String type, Map<String, Object> data) throws IOException {
        PutData putData = new PutData();
        putData.setIndex(index);
        if (StringUtils.isTrimEmpty(type)) {
            putData.setType(DEFAULT_TYPE);
        } else {
            putData.setType(type);
        }
        putData.setJsonMap(data);
        return putDocument(putData);
    }


    @Override
    public boolean deleteDocument(String index, String type, String id) throws IOException {
        BaseData baseData = new BaseData();
        baseData.setIndex(index);
        baseData.setType(type);
        baseData.setDocumentId(id);
        return deleteDocument(baseData);
    }

    @Override
    public boolean deleteDocumentWithRouting(String index, String type, String id, String routing) throws IOException {
        BaseData baseData = new BaseData();
        baseData.setIndex(index);
        baseData.setType(type);
        baseData.setDocumentId(id);
        baseData.setRouting(routing);
        return deleteDocument(baseData);
    }

    @Override
    public boolean deleteDocumentByQuery(String index, Map<String, Object> query) throws IOException {
        if (!indexOrDocumentIsExists(index, null, null)) {
            throw new IllegalStateException("delete fail" + index + "not found");
        }
        String source = JsonUtils.toJSONString(query);
        HttpEntity entity = new NStringEntity(source, ContentType.APPLICATION_JSON);
        try {
            Response response = restClient.performRequest(POST, "/" + index + "/_delete_by_query", Collections.singletonMap("pretty", "true"), entity);

            JSONObject jsonObj = JSON.parseObject(EntityUtils.toString(response.getEntity()));
            return jsonObj.getInteger("total") >= 0 ? true : false;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    @Override
    public boolean deleteDocumentByQuery(String index, Map<String, Object> query, int scrollSize) throws IOException {
        if (!indexOrDocumentIsExists(index, null, null)) {
            throw new IllegalStateException("delete fail" + index + "not found");
        }
        String source = JsonUtils.toJSONString(query);
        HttpEntity entity = new NStringEntity(source, ContentType.APPLICATION_JSON);
        try {
            Response response = restClient.performRequest(POST, "/" + index + "/_delete_by_query?scroll_size=" + scrollSize, Collections.singletonMap("pretty", "true"), entity);

            JSONObject jsonObj = JSON.parseObject(EntityUtils.toString(response.getEntity()));
            return jsonObj.getInteger("total") >= 0 ? true : false;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    @Override
    public boolean deleteDocumentByQuery(String index, String type, Map<String, Object> query) throws IOException {
        if (!indexOrDocumentIsExists(index, null, null)) {
            throw new IllegalStateException("delete fail" + index + "not found");
        }
        String source = JsonUtils.toJSONString(query);
        HttpEntity entity = new NStringEntity(source, ContentType.APPLICATION_JSON);
        try {
            Response response = restClient.performRequest(POST, "/" + index + "/" + type + "/_delete_by_query?conflicts=proceed", Collections.singletonMap("pretty", "true"), entity);

            JSONObject jsonObj = JSON.parseObject(EntityUtils.toString(response.getEntity()));
            return jsonObj.getInteger("total") >= 0 ? true : false;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    @Override
    public boolean deleteDocumentByQueryWithRouting(String index, String routing, Map<String, Object> query) throws IOException {
        if (!indexOrDocumentIsExists(index, null, null)) {
            throw new IllegalStateException("delete fail" + index + "not found");
        }
        String source = JsonUtils.toJSONString(query);
        HttpEntity entity = new NStringEntity(source, ContentType.APPLICATION_JSON);
        try {
            Response response = restClient.performRequest(POST, "/" + index + "/_delete_by_query?routing=" + routing, Collections.singletonMap("pretty", "true"), entity);

            JSONObject jsonObj = JSON.parseObject(EntityUtils.toString(response.getEntity()));
            return jsonObj.getInteger("total") >= 0 ? true : false;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    @Override
    public void updateDocument(String index, String type, String id, Map<String, Object> data) throws IOException {
        UpdateData updateData = new UpdateData();
        updateData.setIndex(index);
        updateData.setType(type);
        updateData.setDocumentId(id);
        updateData.setDocJsonMap(data);
        updateDocument(updateData);
    }

    @Override
    public void updateByQueryDSL(String index, Map<String, Object> queryDSL) throws IOException {
        if (!indexOrDocumentIsExists(index, null, null)) {
            throw new IllegalStateException("update fail" + index + "not found");
        }

        String source = JsonUtils.toJSONString(queryDSL);
        HttpEntity entity = new NStringEntity(source, ContentType.APPLICATION_JSON);
        Response response = restClient.performRequest(POST, "/" + index + "/_update_by_query?conflicts=proceed" , Collections.emptyMap(), entity);
        logger.info(response.getEntity().toString());

    }



    @Override
    public Map getIndexMapping(String index, boolean pretty) throws Exception {
        StringBuilder sb=new StringBuilder();
        if(pretty){
            sb.append(index).append("/_mapping?pretty");
        }else {
            sb.append(index).append("/_mapping");
        }
        Response response = restClient.performRequest(GET,sb.toString());
        return JsonUtils.toMap(EntityUtils.toString(response.getEntity()));
    }

    @Override
    public Map getIndexAll(String index, boolean pretty) throws Exception {
        StringBuilder sb=new StringBuilder();
        if(pretty){
            sb.append(index).append("?pretty");
        }else {
            sb.append(index);
        }
        Response response = restClient.performRequest(GET,sb.toString());
        return JsonUtils.toMap(EntityUtils.toString(response.getEntity()));
    }

    @Override
    public Map getSettings(String index, boolean pretty) throws Exception {
        StringBuilder sb=new StringBuilder();
        if(pretty){
            sb.append(index).append("/_settings?pretty");
        }else {
            sb.append(index);
        }
        Response response = restClient.performRequest(GET,sb.toString());
        return JsonUtils.toMap(EntityUtils.toString(response.getEntity()));
    }

    @Override
    public Map getAlias(String index, boolean pretty) throws Exception {
        StringBuilder sb=new StringBuilder();
        if(pretty){
            sb.append(index);
        }else {
            sb.append(index).append("/_alias?pretty");
        }
        Response response = restClient.performRequest(GET,sb.toString());
        return JsonUtils.toMap(EntityUtils.toString(response.getEntity()));
    }

    @Override
    public List<Map<String, Object>> matchAllQuery(String index, String type, String routing) throws IOException {
       if(!indexOrDocumentIsExists(index,null,null)){
           logger.warn("index:"+index+" is missing");
           return null;
       }
        SearchRequest searchRequest = new SearchRequest(index);
       if(!StringUtils.isTrimEmpty(type)){
           searchRequest.types(type);
       }
        if(!StringUtils.isTrimEmpty(routing)){
            searchRequest.routing(routing);
        }
        searchRequest.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
        SearchResponse response=client.search(searchRequest);
        List<Map<String, Object>> list=new ArrayList<>();
        for (SearchHit documentFields : response.getHits()) {
            list.add( documentFields.getSourceAsMap());

        }
        return list;
    }

    @Override
    public List<Map<String, Object>> matchQuery(String index, String fileName, Object value) throws IOException {
        return commonQuery(index,QueryBuilders.matchQuery(fileName,value));
    }

    @Override
    public List<Map<String, Object>> multiMatchQuery​(String index, Object text, Set<String> fileNames) throws IOException {
        if(fileNames.isEmpty()){
            logger.warn(" The field names is null");
            return null;
        }
        return commonQuery(index,QueryBuilders.multiMatchQuery(text,fileNames.toArray(new String[fileNames.size()])));
    }

    @Override
    public List<Map<String, Object>> matchPhraseQuery​(String index, String fileName, Object text) throws IOException {
        return commonQuery(index,QueryBuilders.matchPhraseQuery(fileName,text));
    }

    @Override
    public List<Map<String, Object>> matchPhrasePrefixQuery(String index, String fileName, Object text) throws IOException {
        return commonQuery(index,QueryBuilders.matchPhrasePrefixQuery(fileName,text));
    }

    @Override
    public List<Map<String, Object>> commonTermsQuery(String index, String fileName, Object text) throws IOException {
        return commonQuery(index,QueryBuilders.commonTermsQuery(fileName,text));
    }

    @Override
    public List<Map<String, Object>> termQuery(String index, String fileName, String value) throws IOException {
        return commonQuery(index,QueryBuilders.termQuery(fileName,value));
    }

    @Override
    public List<Map<String, Object>> termQuery(String index, String fileName, int value) throws IOException {
        return commonQuery(index,QueryBuilders.termQuery(fileName,value));
    }

    @Override
    public List<Map<String, Object>> termQuery(String index, String fileName, float value) throws IOException {
        return commonQuery(index,QueryBuilders.termQuery(fileName,value));
    }

    @Override
    public List<Map<String, Object>> termQuery(String index, String fileName, double value) throws IOException {
        return commonQuery(index,QueryBuilders.termQuery(fileName,value));
    }

    @Override
    public List<Map<String, Object>> termQuery(String index, String fileName, boolean value) throws IOException {
        return commonQuery(index,QueryBuilders.termQuery(fileName,value));
    }

    @Override
    public List<Map<String, Object>> termQuery(String index, String fileName, Object value) throws IOException {
        return commonQuery(index,QueryBuilders.termQuery(fileName,value));
    }

    @Override
    public List<Map<String, Object>> termsQuery(String index, String fileName, Set<String> values) throws IOException {
       if(values.isEmpty()){
           logger.warn("The terms is null");
       }
        return commonQuery(index,QueryBuilders.termsQuery(fileName,values.toArray(new String[values.size()])));
    }

    @Override
    public List<Map<String, Object>> termsQuery(String index, String fileName, int... values) throws IOException {
        return commonQuery(index,QueryBuilders.termsQuery(fileName,values));
    }

    @Override
    public List<Map<String, Object>> termsQuery(String index, String fileName, float... values) throws IOException {
        return commonQuery(index,QueryBuilders.termsQuery(fileName,values));
    }

    @Override
    public List<Map<String, Object>> termsQuery(String index, String fileName, double... values) throws IOException {
        return commonQuery(index,QueryBuilders.termsQuery(fileName,values));
    }

    @Override
    public List<Map<String, Object>> termsQuery(String index, String fileName, Object... values) throws IOException {
        return commonQuery(index,QueryBuilders.termsQuery(fileName,values));
    }

    @Override
    public List<Map<String, Object>> rangeQuery(String index, String fileName, Object from, Object to, boolean includeLower, boolean includeUpper) throws IOException {
        return commonQuery(index,QueryBuilders.rangeQuery(fileName).from(from,includeLower).to(to,includeUpper));
    }

    @Override
    public List<Map<String, Object>> fileExistsQuery(String index, String fileName) throws IOException {
        return commonQuery(index,QueryBuilders.existsQuery(fileName));
    }

    @Override
    public List<Map<String, Object>> prefixQuery​(String index, String fileName, String text) throws IOException {
        return commonQuery(index,QueryBuilders.prefixQuery(fileName,text));
    }

    @Override
    public List<Map<String, Object>> wildcardQuery​(String index, String fileName, String text) throws IOException {
        return commonQuery(index,QueryBuilders.wildcardQuery(fileName,text));
    }

    @Override
    public List<Map<String, Object>> regexpQuery(String index, String fileName, String text) throws IOException {
        return commonQuery(index,QueryBuilders.regexpQuery(fileName,text));
    }

    @Override
    public List<Map<String, Object>> fuzzyQuery​(String index, String fileName, String text) throws IOException {
        return commonQuery(index,QueryBuilders.fuzzyQuery(fileName,text));
    }

    @Override
    public List<Map<String, Object>> fuzzyQuery​(String index, String fileName, Object value) throws IOException {
        return commonQuery(index,QueryBuilders.fuzzyQuery(fileName,value));
    }

    @Override
    public List<Map<String, Object>> typeQuery(String index, String type) throws IOException {
        return commonQuery(index,QueryBuilders.typeQuery(type));
    }

    @Override
    public List<Map<String, Object>> idsQuery(String index, String type, Set<String> ids) throws IOException {
        return commonQuery(index,QueryBuilders.idsQuery(type).addIds(ids.toArray(new String[ids.size()])));
    }

    @Override
    public Map<String, Object> info() throws IOException {
        MainResponse response = client.info();
        Map<String, Object> map=new HashMap<>();
        map.put("clusterName",response.getClusterName());
        map.put("clusterUuid",response.getClusterUuid());
        map.put("version",response.getVersion());
        map.put("nodeName",response.getNodeName());
        map.put("build",response.getBuild());

        return map;
    }


    private List<Map<String,Object>> commonQuery(String index,QueryBuilder queryBuilder) throws IOException {
        if(!indexOrDocumentIsExists(index,null,null)){
            logger.warn("index:"+index+" is missing");
            return null;
        }
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(new SearchSourceBuilder().query(queryBuilder));
        SearchResponse response=client.search(searchRequest);
        List<Map<String, Object>> list=new ArrayList<>();
        for (SearchHit documentFields : response.getHits()) {
            list.add( documentFields.getSourceAsMap());

        }
        return list;
    }


}
