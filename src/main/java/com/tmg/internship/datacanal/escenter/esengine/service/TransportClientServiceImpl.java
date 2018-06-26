package com.tmg.internship.datacanal.escenter.esengine.service;

import com.tmg.utils.PageUtil;
import com.tmg.utils.StringUtils;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author chensl [cookchensl@gmail.com]
 * @date 2018/5/30 15:23
 * @description
 * @since 2.8.1
 */
@Service
public class TransportClientServiceImpl implements TransportClientService {

    private final static Logger logger = LoggerFactory.getLogger(TransportClientServiceImpl.class);

    @Autowired
    private TransportClient transportClient;

    private static TransportClient client;

    private static final String DEFAULT_TYPE = "_doc";

    @PostConstruct
    public void init() {
        client = this.transportClient;
    }

    @PreDestroy
    public void destroy() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public boolean isExists(String index) {
        IndicesExistsResponse response = client.admin().indices().prepareExists(index).execute().actionGet();
        return response.isExists() ? true : false;
    }

    @Override
    public boolean openIndex(String index) {
        if (!isExists(index)) {
            logger.info("open index fail,it's not exists");
            return false;
        } else {
            return client.admin().indices().prepareOpen(index).execute().actionGet().isAcknowledged();
        }
    }

    @Override
    public boolean closeIndex(String index) {
        if (!isExists(index)) {
            logger.info("close index fail,it's not exists");
            return false;
        } else {
            return client.admin().indices().prepareClose(index).execute().actionGet().isAcknowledged();
        }
    }

    @Override
    public void refreshIndex(Set<String> indexs) {
        if (indexs == null) {
            client.admin().indices().prepareRefresh().execute().actionGet();
        } else {
            client.admin().indices()
                    .prepareRefresh(indexs.toArray(new String[indexs.size()]))
                    .execute().actionGet();
        }
    }


    @Override
    public boolean createIndex(String index, String alias) {
        if (StringUtils.isTrimEmpty(index)) {
            throw new IllegalStateException("create index but the indexName is null");
        }
        CreateIndexResponse createIndexResponse = StringUtils.isTrimEmpty(alias) ? client.admin().indices().prepareCreate(index.toLowerCase()).get() : client.admin().indices().prepareCreate(index.toLowerCase()).addAlias(new Alias(alias)).get();
        return createIndexResponse.isAcknowledged();
    }

    @Override
    public boolean createIndexWithSetting(String index, String alias, Map<String, Object> setting) {
        if (StringUtils.isTrimEmpty(index)) {
            throw new IllegalStateException("create index but the indexName is null");
        }
        if (setting.isEmpty()) {
            throw new IllegalStateException("create index with setting  but settings is null");
        }
        CreateIndexResponse createIndexResponse;
        if (StringUtils.isTrimEmpty(alias)) {
            createIndexResponse = client.admin().indices()
                    .prepareCreate(index.toLowerCase())
                    .setSettings(setting)
                    .execute().actionGet();
        } else {
            createIndexResponse = client.admin().indices()
                    .prepareCreate(index.toLowerCase()).addAlias(new Alias(alias))
                    .setSettings(setting)
                    .execute().actionGet();
        }
        return createIndexResponse.isAcknowledged();
    }

    @Override
    public void updateSettings(String index, Map<String, Object> setting) {
        if (!isExists(index)) {
            throw new IllegalStateException("update settings fail : " + index + "is not exist");
        }
        if (setting.isEmpty()) {
            throw new IllegalStateException("update setting  but settings is null");
        }
        client.admin().indices().prepareUpdateSettings(index).setSettings(setting).execute().actionGet();
    }

    @Override
    public void createIndexWitheMapping(String index, String type, String alias, Map<String, Object> mapping) {
        if (StringUtils.isTrimEmpty(index)) {
            throw new IllegalStateException("create index but the indexName is null");
        }
        if (mapping.isEmpty()) {
            throw new IllegalStateException("create index with mapping  but mapping is null");
        }
        if (StringUtils.isTrimEmpty(alias)) {
            client.admin().indices().prepareCreate(index).addMapping(type, mapping).execute().actionGet();
        } else {
            client.admin().indices().prepareCreate(index).addAlias(new Alias(alias)).addMapping(type, mapping).execute().actionGet();
        }
    }

    @Override
    public void putMapping(String index, String type, Map<String, Object> mapping) {
        if (!isExists(index)) {
            throw new IllegalStateException("put mapping fail : " + index + "is not exist");
        }
        if (mapping.isEmpty()) {
            client.admin().indices().preparePutMapping(index)
                    .setType(type)
                    .execute().actionGet();
        } else {
            client.admin().indices().preparePutMapping(index)
                    .setType(type)
                    .setSource(mapping)
                    .execute().actionGet();
        }
    }

    @Override
    public void createIndexWithSettingAndMapping(String index, String type, String alias, Map<String, Object> setting, Map<String, Object> mapping) {
        if (StringUtils.isTrimEmpty(index)) {
            throw new IllegalStateException("create index but the indexName is null");
        }
        if (StringUtils.isTrimEmpty(type)) {
            throw new IllegalStateException("create index with setting and mapping fail ,type is null");
        }
        if (setting.isEmpty()) {
            throw new IllegalStateException("create index with setting and mapping fail ,setting is null");
        }
        if (mapping.isEmpty()) {
            throw new IllegalStateException("create index with setting and mapping fail ,mapping is null");
        }

        if (StringUtils.isTrimEmpty(alias)) {
            client.admin().indices().prepareCreate(index).setSettings(setting).addMapping(type, mapping).execute().actionGet();
        } else {
            client.admin().indices().prepareCreate(index).addAlias(new Alias(alias)).setSettings(setting).addMapping(type, mapping).execute().actionGet();
        }
    }

    @Override
    public boolean deleteIndex(String index) {
        if (!isExists(index)) {
            throw new IllegalStateException("delete index fail : " + index + "is not exist");
        }
        DeleteIndexResponse deleteResponse = client.admin().indices()
                .prepareDelete(index.toLowerCase())
                .execute()
                .actionGet();
        return deleteResponse.isAcknowledged() ? true : false;
    }

    @Override
    public boolean deleteDocument(String index, String type, String fileId) {
        DeleteResponse response = client.prepareDelete(index, type, fileId).execute().actionGet();
        return "deleted".equalsIgnoreCase(response.getResult().toString()) ? true : false;
    }

    @Override
    public long deleteByQuery(String index, QueryBuilder filter) {
        if (!isExists(index)) {
            throw new IllegalStateException("delete index fail : " + index + "is not exist");
        }
        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(filter)
                .source(index)
                .execute().actionGet();
        return response.getDeleted();
    }

    @Override
    public void update(String index, String type, String fileId, Map<String, Object> doc) throws ExecutionException, InterruptedException {
        if (!isExists(index)) {
            throw new IllegalStateException("update data fail : " + index + "is not exist");
        }
        UpdateRequest updateRequest = new UpdateRequest(index, type, fileId);
        updateRequest.doc(doc);
        client.update(updateRequest).get();
    }

    @Override
    public void updateByScript(String index, String type, String fileId, String scriptStr) throws ExecutionException, InterruptedException {
        if (!isExists(index)) {
            throw new IllegalStateException("update data fail : " + index + "is not exist");
        }
        UpdateRequest updateRequest = new UpdateRequest(index, type, fileId)
                .script(new Script(scriptStr));
        client.update(updateRequest).get();
    }

    @Override
    public void updateByUpsert(String index, String type, String fileId, Map<String, Object> doc) throws ExecutionException, InterruptedException {
        if (!isExists(index)) {
            throw new IllegalStateException("update data fail : " + index + "is not exist");
        }
        IndexRequest indexRequest = new IndexRequest(index, type, fileId).source(doc);
        UpdateRequest updateRequest = new UpdateRequest(index, type, fileId).doc(doc).upsert(indexRequest);
        client.update(updateRequest).get();
    }


    @Override
    public boolean updateByQuery(String index, int size,Map<String,Object> params, QueryBuilder filter) {
        if (!isExists(index)) {
            throw new IllegalStateException("update data fail : " + index + " is not exist");
        }
        if (size <= 0) {
            size = 1000;
        }
        UpdateByQueryRequestBuilder updateByQuery = UpdateByQueryAction.INSTANCE.newRequestBuilder(client);
        if(params.isEmpty()){
            throw new IllegalStateException("update data fail ,params is null");
        }
        StringBuilder stringBuilder=new StringBuilder();
        for (String s : params.keySet()) {
            stringBuilder.append("ctx._source."+s+"=params."+s+";");
        }
        Script script = new Script(ScriptType.INLINE,Script.DEFAULT_SCRIPT_LANG, stringBuilder.toString(), Collections.emptyMap(),params);
        updateByQuery.source(index)
                .filter(filter)
                .size(size)
                .script(script)
                .abortOnVersionConflict(false);
        BulkByScrollResponse response = updateByQuery.get();
        StringBuilder sb = new StringBuilder();
        for (BulkItemResponse.Failure failure : response.getBulkFailures()) {
            sb.append(failure.getMessage() + "\n");
        }
        if (sb.length() > 0) {
            throw new IllegalStateException(sb.toString());
        } else {
            return true;
        }
    }

    @Override
    public void postData(String index, String type, String id, Map<String, Object> doc) {
        if (StringUtils.isTrimEmpty(type)) {
            type = DEFAULT_TYPE;
        }
        if (doc.isEmpty()) {
            if (StringUtils.isTrimEmpty(id)) {
                client.prepareIndex(index, type).setSource().execute().actionGet();
            } else {
                client.prepareIndex(index, type, id).setSource().execute().actionGet();
            }
        } else {
            if (StringUtils.isTrimEmpty(id)) {
                client.prepareIndex(index, type).setSource(doc).execute().actionGet();
            } else {
                client.prepareIndex(index, type, id).setSource(doc).execute().actionGet();
            }
        }
    }

    @Override
    public void bulkInsertData(String index, String type, List<Map<String, Object>> docs) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        if (StringUtils.isTrimEmpty(type)) {
            type = DEFAULT_TYPE;
        }
        for (Map<String, Object> doc : docs) {
            bulkRequest.add(client.prepareIndex(index, type)
                    .setSource(doc)
            );
        }

        bulkRequest.execute().actionGet();
    }

    @Override
    public Map<String, Object> getSourceById(String index, String type, String id, String fields) {
        if(!isExists(index)){
            logger.warn("getSourceById fail,index " +index +" not exists");
            return null;
        }
        try {
            GetResponse getResponse = client.prepareGet(index, type, id).setFetchSource(fields.split(","), null).execute().actionGet();
            return getResponse.getSource();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public List<Map<String,Object>> matchAllQuery(String index) throws Exception {
        if(!isExists(index)){
            logger.warn("matchAllQuery fail,index " +index +" not exists");
            return null;
        }
        SearchResponse response = client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).get();
        List<Map<String,Object>> list=new ArrayList<>();
        for (SearchHit documentFields : response.getHits()) {
            list.add(documentFields.getSourceAsMap());
        }
        return list;
    }

    @Override
    public int count(String index) throws Exception {
        if(!isExists(index)){
            return 0;
        }
        SearchResponse response = client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).get();
        return (int)response.getHits().getTotalHits();
    }

    @Override
    public List<Map<String, Object>> matchAllQueryWithPage(String index, PageUtil pageUtil) throws Exception {
        if(!isExists(index)){
            logger.warn("matchAllQueryWithPage fail,index " +index +" not exists");
            return null;
        }
        SearchResponse response = client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setFrom(pageUtil.getStart()).setSize(pageUtil.getRows()).get();
        List<Map<String,Object>> list=new ArrayList<>();
        for (SearchHit documentFields : response.getHits()) {
            list.add(documentFields.getSourceAsMap());
        }
        return list;
    }

    @Override
    public List<Map<String, Object>> queryByCondition(String index, QueryBuilder queryBuilder) throws Exception {
        if(!isExists(index)){
            logger.warn("queryByCondition fail,index " +index +" not exists");
            return null;
        }
        SearchResponse response = client.prepareSearch(index).setQuery(queryBuilder).get();
        List<Map<String,Object>> list=new ArrayList<>();
        for (SearchHit documentFields : response.getHits()) {
            list.add(documentFields.getSourceAsMap());
        }
        return list;
    }

    @Override
    public int countByCondition(String index, QueryBuilder queryBuilder) throws Exception {
        if(!isExists(index)){
            return 0;
        }
        SearchResponse response = client.prepareSearch(index).setQuery(queryBuilder).get();
        return (int)response.getHits().getTotalHits();
    }

    @Override
    public List<Map<String, Object>> queryByConditionWithPage(String index, PageUtil pageUtil, QueryBuilder queryBuilder) throws Exception {
        if(!isExists(index)){
            logger.warn("queryByConditionWithPage fail,index " +index +" not exists");
            return null;
        }
        SearchResponse response = client.prepareSearch(index).setQuery(queryBuilder).setFrom(pageUtil.getStart()).setSize(pageUtil.getRows()).get();
        List<Map<String,Object>> list=new ArrayList<>();
        for (SearchHit documentFields : response.getHits()) {
            list.add(documentFields.getSourceAsMap());
        }
        return list;
    }

    @Override
    public List<Map<String, Object>> mustQueryByCondition(String index, Map<String, Object> params) {
        if(!isExists(index)){
            logger.warn("mustQueryByCondition fail,index " +index +" not exists");
            return null;
        }
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (String s : params.keySet()) {
            if(params.get(s)==null){
                boolQuery.mustNot(QueryBuilders.existsQuery(s));
            }else {
                boolQuery.must(QueryBuilders.termQuery(s,params.get(s)));
            }

        }
        SearchResponse response = client.prepareSearch(index).setQuery(boolQuery).get();
        List<Map<String,Object>> list=new ArrayList<>();
        for (SearchHit documentFields : response.getHits()) {
            list.add(documentFields.getSourceAsMap());
        }
        return list;
    }

    @Override
    public List<Map<String, Object>> shouldQueryByCondition(String index, Map<String, Object> params) {
        if(!isExists(index)){
            logger.warn("shouldQueryByCondition fail,index " +index +" not exists");
            return null;
        }
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (String s : params.keySet()) {
            boolQuery.should(QueryBuilders.termQuery(s,params.get(s)));
        }
        SearchResponse response = client.prepareSearch(index).setQuery(boolQuery).get();
        List<Map<String,Object>> list=new ArrayList<>();
        for (SearchHit documentFields : response.getHits()) {
            list.add(documentFields.getSourceAsMap());
        }
        return list;
    }

    @Override
    public List<Map<String, Object>> mustNotQueryByCondition(String index, Map<String, Object> params) {
        if(!isExists(index)){
            logger.warn("mustNotQueryByCondition fail,index " +index +" not exists");
            return null;
        }
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (String s : params.keySet()) {
            boolQuery.mustNot(QueryBuilders.termQuery(s,params.get(s)));
        }
        SearchResponse response = client.prepareSearch(index).setQuery(boolQuery).get();
        List<Map<String,Object>> list=new ArrayList<>();
        for (SearchHit documentFields : response.getHits()) {
            list.add(documentFields.getSourceAsMap());
        }
        return list;
    }

    @Override
    public List<Map<String, Object>> query(String index, Map<String, Object> must, Map<String, Object> should, Map<String, Object> not) {
        if(!isExists(index)){
            logger.warn("query fail,index " +index +" not exists");
            return null;
        }
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (String s : not.keySet()) {
            boolQuery.mustNot(QueryBuilders.termQuery(s,not.get(s)));
        }
        for (String s : should.keySet()) {
            boolQuery.should(QueryBuilders.termQuery(s,not.get(s)));
        }
        for (String s : must.keySet()) {
            boolQuery.must(QueryBuilders.termQuery(s,not.get(s)));
        }

        SearchResponse response = client.prepareSearch(index).setQuery(boolQuery).get();
        List<Map<String,Object>> list=new ArrayList<>();
        for (SearchHit documentFields : response.getHits()) {
            list.add(documentFields.getSourceAsMap());
        }
        return list;
    }

    @Override
    public List<Map<String, Object>> termQuery(String index, String fileId, Object value) {
        if(!isExists(index)){
            logger.warn("termQuery fail,index " +index +" not exists");
            return null;
        }
        SearchResponse response = client.prepareSearch(index).setQuery(QueryBuilders.termQuery(fileId,value)).get();
        List<Map<String,Object>> list=new ArrayList<>();
        for (SearchHit documentFields : response.getHits()) {
            list.add(documentFields.getSourceAsMap());
        }
        return list;
    }

    @Override
    public List<Map<String, Object>> termsQuery(String index, String fileId, Set<Object> values) {
        if(!isExists(index)){
            logger.warn("termsQuery fail,index " +index +" not exists");
            return null;
        }
        SearchResponse response = client.prepareSearch(index).setQuery(QueryBuilders.termsQuery(fileId,values)).get();
        List<Map<String,Object>> list=new ArrayList<>();
        for (SearchHit documentFields : response.getHits()) {
            list.add(documentFields.getSourceAsMap());
        }
        return list;
    }

    @Override
    public List<Map<String, Object>> existsQuery(String index,String fileId) {
        if(!isExists(index)){
            logger.warn("existsQuery fail,index " +index +" not exists");
            return null;
        }
        SearchResponse response = client.prepareSearch(index).setQuery(QueryBuilders.existsQuery(fileId)).get();
        List<Map<String,Object>> list=new ArrayList<>();
        for (SearchHit documentFields : response.getHits()) {
            list.add(documentFields.getSourceAsMap());
        }
        return list;
    }

    @Override
    public List<Map<String, Object>> multiMatchQuery(String index, Object value, Set<String> fileId) {
        if(!isExists(index)){
            logger.warn("multiMatchQuery fail,index " +index +" not exists");
            return null;
        }
        if(fileId.isEmpty()){
            throw new IllegalStateException("fileId is null ");
        }
        SearchResponse response = client.prepareSearch(index).setQuery(QueryBuilders.multiMatchQuery(value,fileId.toArray(new String[fileId.size()]))).get();
        List<Map<String,Object>> list=new ArrayList<>();
        for (SearchHit documentFields : response.getHits()) {
            list.add(documentFields.getSourceAsMap());
        }
        return list;
    }

    @Override
    public List<Map<String, Object>> fuzzyQuery(String index, String fileId, Object value) {
        if(!isExists(index)){
            logger.warn("fuzzyQuery fail,index " +index +" not exists");
            return null;
        }

        SearchResponse response = client.prepareSearch(index).setQuery(QueryBuilders.fuzzyQuery(fileId,value)).get();
        List<Map<String,Object>> list=new ArrayList<>();
        for (SearchHit documentFields : response.getHits()) {
            list.add(documentFields.getSourceAsMap());
        }
        return list;
    }

    @Override
    public List<Map<String, Object>> prefixQuery(String index, String fileId, String prefix) {
        if(!isExists(index)){
            logger.warn("prefixQuery fail,index " +index +" not exists");
            return null;
        }

        SearchResponse response = client.prepareSearch(index).setQuery(QueryBuilders.prefixQuery(fileId,prefix)).get();
        List<Map<String,Object>> list=new ArrayList<>();
        for (SearchHit documentFields : response.getHits()) {
            list.add(documentFields.getSourceAsMap());
        }
        return list;
    }

    @Override
    public List<Map<String, Object>> rangeQuery(String index, String fileId, Object from, Object to, boolean includeLower, boolean includeUpper) {
        if(!isExists(index)){
            logger.warn("rangeQuery fail,index " +index +" not exists");
            return null;
        }

        SearchResponse response = client.prepareSearch(index).setQuery(QueryBuilders.rangeQuery(fileId).from(from,includeLower).to(to,includeUpper)).get();
        List<Map<String,Object>> list=new ArrayList<>();
        for (SearchHit documentFields : response.getHits()) {
            list.add(documentFields.getSourceAsMap());
        }
        return list;
    }

    @Override
    public List<Map<String, Object>> wildcardQuery(String index, String fileId, String wildcard) {
        if(!isExists(index)){
            logger.warn("wildcardQuery fail,index " +index +" not exists");
            return null;
        }

        SearchResponse response = client.prepareSearch(index).setQuery(QueryBuilders.wildcardQuery(fileId,wildcard)).get();
        List<Map<String,Object>> list=new ArrayList<>();
        for (SearchHit documentFields : response.getHits()) {
            list.add(documentFields.getSourceAsMap());
        }
        return list;
    }

    @Override
    public List<Map<String, Object>> regexpQuery(String index, String fileId, String regexp) {
        if(!isExists(index)){
            logger.warn("regexpQuery fail,index " +index +" not exists");
            return null;
        }

        SearchResponse response = client.prepareSearch(index).setQuery(QueryBuilders.regexpQuery(fileId,regexp)).get();
        List<Map<String,Object>> list=new ArrayList<>();
        for (SearchHit documentFields : response.getHits()) {
            list.add(documentFields.getSourceAsMap());
        }
        return list;
    }

    @Override
    public List<Map<String,Object>> spanFirstQuery(String index,String fileId,String value,int maxEnd) {
        if(!isExists(index)){
            logger.warn("spanFirstQuery fail,index " +index +" not exists");
            return null;
        }

        SearchResponse response = client.prepareSearch(index).setQuery(QueryBuilders.spanFirstQuery(QueryBuilders.spanTermQuery(fileId,value),maxEnd)).get();
        List<Map<String,Object>> list=new ArrayList<>();
        for (SearchHit documentFields : response.getHits()) {
            list.add(documentFields.getSourceAsMap());
        }
        return list;
    }

}
