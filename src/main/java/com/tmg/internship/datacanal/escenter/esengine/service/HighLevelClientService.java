package com.tmg.internship.datacanal.escenter.esengine.service;


import com.tmg.internship.datacanal.escenter.esengine.model.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * @author chensl [cookchensl@gmail.com]
 * @Date 2018/5/11 11:23
 */

public interface HighLevelClientService {

    /**
     * 创建索引
     *
     * @param index 索引名
     * @return boolean
     * @throws IOException
     */
    boolean createIndex(String index) throws IOException;

    /**
     * 创建索引
     *
     * @param index   索引名
     * @param setting 设置
     * @param type    类型 默认"_doc"
     * @param mapping 映射
     * @param alias   别名
     * @return boolean
     * @throws IOException
     */
    boolean createIndex(String index, Map<String, Object> setting, String type, Map<String, Object> mapping, String alias) throws IOException;

    /**
     * 创建索引
     *
     * @param index   索引名
     * @param setting 设置
     * @return boolean
     * @throws IOException
     */
    boolean createIndex(String index, Map<String, Object> setting) throws IOException;

    /**
     * 创建索引
     *
     * @param index   索引名
     * @param setting 设置
     * @param alias   别名
     * @return boolean
     * @throws IOException
     */
    boolean createIndex(String index, Map<String, Object> setting, String alias) throws IOException;

    /**
     * 创建索引
     *
     * @param index   索引名
     * @param mapping 映射
     * @param type    类型  默认"_doc"
     * @param alias   别名
     * @return boolean
     * @throws IOException
     */
    boolean createIndex(String index, Map<String, Object> mapping, String type, String alias) throws IOException;

    /**
     * 删除索引
     *
     * @param index 索引名
     * @return boolean
     * @throws IOException
     */
    boolean deleteIndex(String index) throws IOException;

    /**
     * 打开索引
     *
     * @param index 索引名
     * @throws IOException
     * @return boolean
     */
    boolean openIndex(String index) throws IOException;

    /**
     * 关闭索引
     *
     * @param index 索引名
     * @throws IOException
     * @return boolean
     */
    boolean closeIndex(String index) throws IOException;


    /**
     * 创建索引
     *
     * @param createIndex {@link CreateIndex}
     * @throws IOException
     * @return boolean
     */
    boolean createIndex(CreateIndex createIndex) throws IOException;


    /**
     * 删除索引
     *
     * @param baseIndex {@link BaseIndex}
     * @throws IOException
     * @return boolean
     */
    boolean deleteIndex(BaseIndex baseIndex) throws IOException;


    /**
     * 打开索引
     *
     * @param baseIndex {@link BaseIndex}
     * @throws IOException
     * @return boolean
     */
    boolean openIndex(BaseIndex baseIndex) throws IOException;

    /**
     * 关闭索引
     *
     * @param baseIndex {@link BaseIndex}
     * @throws IOException
     * @return boolean
     */
    boolean closeIndex(BaseIndex baseIndex) throws IOException;


    /**
     * 创建文档
     *
     * @param data {@link PutData}
     * @throws IOException
     * @return boolean
     */
    boolean putDocument(PutData data) throws IOException;

    /**
     * 是否存在
     *
     * @param indexName
     * @param typeName
     * @param fileId
     * @throws IOException
     * @return boolean
     */
    boolean isExists(String indexName, String typeName, String fileId) throws IOException;


    /**
     * 删除文档
     *
     * @param baseData {@link BaseData}
     * @throws IOException
     * @return boolean
     */
    boolean deleteDocument(BaseData baseData) throws IOException;

    /**
     * 修改文档
     *
     * @param updateData {@link UpdateData}
     * @throws IOException
     */
    void updateDocument(UpdateData updateData) throws IOException;


    /**
     * 更新mapping
     *
     * @param putMapping {@link PutMapping}
     * @throws IOException
     * @return boolean
     */
    boolean putMapping(PutMapping putMapping) throws IOException;

    /**
     * 更新mapping
     *
     * @param index  索引
     * @param type   类型
     * @param source mapping
     * @throws IOException
     * @return boolean
     */
    boolean putMapping(String index, String type, Map<String, Object> source) throws IOException;


    /**
     * 修改settings
     *
     * @param index    索引
     * @param settings settings
     * @throws IOException
     * @return boolean
     */
    boolean updateSettings(String index, Map<String, Object> settings) throws IOException;

    /**
     * 判断是否存在
     *
     * @param index
     * @param type
     * @param documentId
     * @throws IOException
     * @return boolean
     */
    boolean indexOrDocumentIsExists(String index, String type, String documentId) throws IOException;

    /**
     * 删除索引或者type或者文档
     *
     * @param index 索引名
     * @param type 类型名
     * @param id 文档id
     * @throws IOException
     * @return boolean
     */
    boolean delete(String index, String type, String id) throws IOException;

    /**
     * 执行多个索引，更新和/或删除操作。
     *
     * @param putDataList
     * @throws IOException
     */
    void bulkData(List<PutData> putDataList) throws IOException;


    /**
     * 根据文档id修改数据
     *
     * @param index
     * @param type
     * @param id
     * @param data
     * @throws IOException
     * @return boolean
     */
    boolean putDocumentWithId(String index, String type, String id, Map<String, Object> data) throws IOException;

    /**
     * 根据version修改数据
     *
     * @param index
     * @param type
     * @param id
     * @param version
     * @param data
     * @return boolean
     * @throws IOException
     */
    boolean putDocumentWithVersion(String index, String type, String id, long version, Map<String, Object> data) throws IOException;

    /**
     * 根据路由修改数据
     *
     * @param index
     * @param type
     * @param routing
     * @param data
     * @return boolean
     * @throws IOException
     */
    boolean postDocumentWithRouting(String index, String type, String routing, Map<String, Object> data) throws IOException;

    /**
     * 添加数据
     *
     * @param index
     * @param type
     * @param data
     * @return
     * @throws IOException
     */
    boolean postDocument(String index, String type, Map<String, Object> data) throws IOException;

    /**
     * 删除数据
     *
     * @param index
     * @param type
     * @param id
     * @return boolean
     * @throws IOException
     */
    boolean deleteDocument(String index, String type, String id) throws IOException;

    /**
     * 根据路由删除数据
     *
     * @param index
     * @param type
     * @param id
     * @param routing
     * @return boolean
     * @throws IOException
     */
    boolean deleteDocumentWithRouting(String index, String type, String id, String routing) throws IOException;

    /**
     * 根据查询条件删除数据
     *
     * @param index
     * @param query
     * @return boolean
     * @throws IOException
     */
    boolean deleteDocumentByQuery(String index, Map<String, Object> query) throws IOException;

    /**
     * 根据查询条件删除数据，可设置批处理大小
     *
     * @param index
     * @param query
     * @param scrollSize 默认情况下是1000
     * @return boolean
     * @throws IOException
     */
    boolean deleteDocumentByQuery(String index, Map<String, Object> query, int scrollSize) throws IOException;

    /**
     * 根据条件删除数据
     *
     * @param index
     * @param type
     * @param query
     * @return boolean
     * @throws IOException
     */
    boolean deleteDocumentByQuery(String index, String type, Map<String, Object> query) throws IOException;

    /**
     * 根据查询条件和路由删除数据
     *
     * @param index
     * @param routing
     * @param query
     * @return
     * @throws IOException
     */
    boolean deleteDocumentByQueryWithRouting(String index, String routing, Map<String, Object> query) throws IOException;

    /**
     * 修改数据
     *
     * @param index
     * @param type
     * @param id
     * @param data
     * @throws IOException
     */
    void updateDocument(String index, String type, String id, Map<String, Object> data) throws IOException;

    /**
     * 根据条件修改
     *
     * @param index
     * @param queryDSL
     * @throws IOException
     */
    void updateByQueryDSL(String index,Map<String,Object> queryDSL) throws IOException;


    /**
     * 获取mapping
     *
     * @param index  索引名
     * @param pretty 是否美化
     * @return
     * @throws Exception
     */
    Map getIndexMapping(String index, boolean pretty) throws Exception;


    /**
     * 获取所有
     *
     * @param index
     * @param pretty
     * @return
     * @throws Exception
     */
    Map getIndexAll(String index, boolean pretty) throws Exception;

    /**
     * 获取settings
     *
     * @param index
     * @param pretty
     * @return
     * @throws Exception
     */
    Map getSettings(String index, boolean pretty) throws Exception;

    /**
     * 获取别名
     *
     * @param index
     * @param pretty
     * @return
     * @throws Exception
     */
    Map getAlias(String index, boolean pretty) throws Exception;


    /**
     * 查询全部
     *
     * @param index  索引名
     * @param type  类型
     * @param routing 路由
     * @return
     */
    List<Map<String,Object>>  matchAllQuery(String index,String type,String routing) throws IOException;


    /**
     * 匹配查询
     *
     * @param index 索引名
     * @param fileName 字段名
     * @param value 查询文本
     * @return
     */
    List<Map<String,Object>> matchQuery(String index,String fileName,Object value) throws IOException;


    /**
     * 多匹配查询
     *
     * @param index 索引名
     * @param text 匹配文本
     * @param fileNames  字段名称集合
     * @return
     * @throws IOException
     */
    List<Map<String,Object>> multiMatchQuery​(String index ,Object text,Set<String> fileNames) throws IOException;


    /**
     * 匹配短语查询
     *
     * @param index 索引名
     * @param fileName 字段名称
     * @param text 短语
     * @return
     * @throws IOException
     */
    List<Map<String,Object>> matchPhraseQuery​(String index,String fileName,Object text) throws IOException;


    /**
     * 匹配短语前缀查询
     *
     * @param index 索引名
     * @param fileName 字段名称
     * @param text 前缀
     * @return
     * @throws IOException
     */
    List<Map<String,Object>> matchPhrasePrefixQuery(String index,String fileName,Object text) throws IOException;


    /**
     *
     *
     * @param index
     * @param fileName
     * @param text
     * @return
     * @throws IOException
     */
    List<Map<String,Object>> commonTermsQuery(String index,String fileName,Object text) throws IOException;


    /**
     * 与包含术语的文档匹配的查询
     *
     * @param index
     * @param fileName
     * @param value
     * @return
     * @throws IOException
     */
    List<Map<String,Object>> termQuery(String index,String fileName,String value)throws IOException;

    List<Map<String,Object>> termQuery(String index,String fileName,int value)throws IOException;

    List<Map<String,Object>> termQuery(String index,String fileName,float value)throws IOException;

    List<Map<String,Object>> termQuery(String index,String fileName,double value)throws IOException;

    List<Map<String,Object>> termQuery(String index,String fileName,boolean value)throws IOException;

    List<Map<String,Object>> termQuery(String index,String fileName,Object value)throws IOException;

    /**
     * 相当于 sql：where user_id in (1,2,3,4)
     *
     * @param index
     * @param fileName
     * @param values
     * @return
     */
    List<Map<String,Object>> termsQuery(String index,String fileName,Set<String> values) throws IOException;

    List<Map<String,Object>> termsQuery(String index,String fileName,int... values) throws IOException;

    List<Map<String,Object>> termsQuery(String index,String fileName,float... values) throws IOException;

    List<Map<String,Object>> termsQuery(String index,String fileName,double... values) throws IOException;

    List<Map<String,Object>> termsQuery(String index,String fileName,Object... values) throws IOException;

    /**
     * 范围查询
     *
     * @param index
     * @param fileName
     * @param from
     * @param to
     * @param includeLower 包括下界 <=
     * @param includeUpper 包括上界 >=
     * @return
     */
    List<Map<String,Object>> rangeQuery(String index,String fileName,Object from,Object to,boolean includeLower,boolean includeUpper) throws IOException;


    /**
     * 相当于sql: where  user_id is not null
     *
     * @param index
     * @param fileName
     * @return
     * @throws IOException
     */
    List<Map<String,Object>> fileExistsQuery(String index,String fileName)throws IOException;

    /**
     * 前缀查询
     *
     * @param index
     * @param fileName
     * @param text
     * @return
     * @throws IOException
     */
    List<Map<String,Object>> prefixQuery​(String index,String fileName,String text)throws IOException;


    /**
     * 通配符查询
     *
     * @param index
     * @param fileName
     * @param text ?代表单个字符，*代表0个或多个字符
     * @return
     * @throws IOException
     */
    List<Map<String,Object>> wildcardQuery​(String index,String fileName,String text)throws IOException;

    /**
     * 正则表达式查询
     *
     * @param index
     * @param fileName
     * @param text
     * @return
     * @throws IOException
     */
    List<Map<String,Object>> regexpQuery(String index,String fileName,String text)throws IOException;

    /**
     * 模糊查询
     *
     * @param index
     * @param fileName
     * @param text
     * @return
     * @throws IOException
     */
    List<Map<String,Object>> fuzzyQuery​(String index,String fileName,String text)throws IOException;

    List<Map<String,Object>> fuzzyQuery​(String index,String fileName,Object value)throws IOException;

    /**
     * A filter based on doc/mapping type.
     *
     * @param index
     * @param type
     * @return
     * @throws IOException
     */
    List<Map<String,Object>> typeQuery(String index,String type)throws IOException;

    /**
     * 相当于sql where id in (1,3,4)
     *
     * @param index
     * @param type
     * @param ids
     * @return
     * @throws IOException
     */
    List<Map<String,Object>> idsQuery(String index,String type,Set<String>ids)throws IOException;

    /**
     * 获取集群信息
     *
     * <pre>
     *     clusterName:以ClusterName的形式检索集群的名称<br>
     *     clusterUuid:集群唯一标识符<br>
     *     nodeName:执行请求的节点的名称<br>
     *     version:执行请求的节点版本<br>
     *     build:执行请求的节点的构建信息
     * </pre>
     * @throws IOException
     * @return
     */
    Map<String,Object> info() throws IOException;

}
