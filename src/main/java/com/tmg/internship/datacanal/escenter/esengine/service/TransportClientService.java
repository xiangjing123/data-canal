package com.tmg.internship.datacanal.escenter.esengine.service;

import com.tmg.utils.PageUtil;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * @author chensl
 * @Email cookchensl@gmail.com
 * @company tmg
 * @Date 2018/5/30 15:54
 * <p>es传输客户端接口api</p>
 */
public interface TransportClientService {

    /**
     * 判断索引是否存在
     *
     * @param index 索引名 必需项
     * @return boolean
     */
    boolean isExists(String index);

    /**
     * 打开索引
     *
     * @param index 索引名
     * @return boolean
     */
    boolean openIndex(String index);

    /**
     * 关闭索引
     *
     * @param index 索引名
     * @return boolean
     */
    boolean closeIndex(String index );

    /**
     * 刷新索引
     * <p>
     * <p>当indexs为null时，刷新所有索引</p>
     *
     * @param indexs 索引集合
     */
    void refreshIndex(Set<String> indexs);


    /**
     * 创建索引，并设置别名
     *
     * @param index 索引名 必需项
     * @param alias 别名 非必须项，不设置可填null
     * @return boolean
     */
    boolean createIndex(String index, String alias);


    /**
     * 创建索引并配置setting
     *
     * @param index   索引名
     * @param alias 别名 非必须项，不设置可填null
     * @param setting 配置
     * @return boolean
     */
    boolean createIndexWithSetting(String index, String alias, Map<String, Object> setting);


    /**
     * 修改已存在索引的setting
     *
     * @param index   索引名
     * @param setting 配置
     */
    void updateSettings(String index, Map<String, Object> setting);


    /**
     * 创建索引并添加mapping
     *
     * @param index   索引名
     * @param type    类型名
     * @param alias 别名 非必须项，不设置可填null
     * @param mapping 映射
     */
    void createIndexWitheMapping(String index, String type, String alias, Map<String, Object> mapping);


    /**
     * 更新索引的mapping
     *
     * @param index   索引名
     * @param type    类型名
     * @param mapping 映射 mapping为空时代表删除该索引下该类型的映射
     * @return
     */
    void putMapping(String index, String type, Map<String, Object> mapping);


    /**
     * 创建索引，同时设置setting和添加映射
     *
     * @param index 索引名
     * @param type 类型名
     * @param setting 设置
     * @param alias 别名 非必须项，不设置可填null
     * @param mapping 映射
     */
    void createIndexWithSettingAndMapping(String index, String type,String alias, Map<String, Object> setting, Map<String, Object> mapping);

    /**
     * 删除索引
     *
     * @param index 索引名
     * @return boolean
     */
    boolean deleteIndex(String index);

    /**
     * 删除文档
     *
     * @param index  索引名
     * @param type   类型名
     * @param fileId 文档id
     * @return boolean
     */
    boolean deleteDocument(String index, String type, String fileId);

    /**
     * 根据查询结果删除
     *
     * @param index  索引名
     * @param filter 条件构造器
     * @return boolean
     */
    long deleteByQuery(String index, QueryBuilder filter);

    /**
     * 更新数据
     *
     * @param index  索引名
     * @param type   类型名
     * @param fileId 文档id
     * @param doc    文档内容  eg doc.put("age",12)
     * @throws ExecutionException
     * @throws InterruptedException
     */
    void update(String index, String type, String fileId, Map<String, Object> doc) throws ExecutionException, InterruptedException;


    /**
     * 根据脚本更新数据
     *
     * @param index     索引名
     * @param type      类型名
     * @param fileId    文档id
     * @param scriptStr 脚本字符串
     * @throws ExecutionException
     * @throws InterruptedException
     */
    void updateByScript(String index, String type, String fileId, String scriptStr) throws ExecutionException, InterruptedException;


    /**
     * 如果文档不存在，则新建一个文档，如果文档存在，则覆盖原文档
     *
     * @param index  索引名
     * @param type   类型名
     * @param fileId 文档id
     * @param doc    文档内容 eg  doc.put("age",12) doc.put("name","Joe Smith")
     * @throws ExecutionException
     * @throws InterruptedException
     */
    void updateByUpsert(String index, String type, String fileId, Map<String, Object> doc) throws ExecutionException, InterruptedException;


    /**
     * 根据条件更新数据
     *
     * @param index     索引名
     * @param size      默认1000
     * @param filter    查询条件
     * @param params  key为要修改的字段，value为要修改的值
     * @return boolean
     */
    boolean updateByQuery(String index, int size, Map<String,Object> params, QueryBuilder filter);

    /**
     * 插入数据
     *
     * @param index 索引名
     * @param type 为null时默认"_doc"
     * @param id 为null时为es自创id
     * @param doc 文档内容
     */
    void postData(String index,String type,String id,Map<String,Object> doc);


    /**
     * 批量插入数据
     *
     * @param index 索引名
     * @param type 类型名 为null时默认"_doc"
     * @param docs 文档集合
     */
    void bulkInsertData(String index, String type, List<Map<String,Object>> docs);


    /**
     * 通过文档id获取文档内容
     *
     * @param index 索引名
     * @param type 类型名
     * @param id 文档id
     * @param fields 需要获取的列，以逗号分隔
     * @return 文档内容
     */
    Map<String, Object> getSourceById(String index, String type, String id, String fields);

    /**
     * 查询某索引下的全部数据
     *
     * @param index 索引名
     * @return
     * @throws Exception
     */
    List<Map<String,Object>> matchAllQuery(String index) throws Exception;

    /**
     * 返回某索引的总文档数
     *
     * @param index
     * @return
     * @throws Exception
     */
    int count(String index) throws Exception;

    /**
     * 分页查询全部
     *
     * @param index 索引名
     * @param pageUtil
     * @return
     * @throws Exception
     */
    List<Map<String,Object>> matchAllQueryWithPage(String index, PageUtil pageUtil) throws Exception;


    /**
     * 根据查询条件查询索引文档数据
     *
     * @param index
     * @param queryBuilder
     * @return
     * @throws Exception
     */
    List<Map<String,Object>> queryByCondition(String index,QueryBuilder queryBuilder) throws Exception;

    /**
     * 根据查询条件得到索引文档总数
     *
     * @param index
     * @param queryBuilder
     * @return
     * @throws Exception
     */
    int countByCondition(String index,QueryBuilder queryBuilder)throws Exception;

    /**
     * 根据查询条件得到索引文档并分页
     *
     * @param index
     * @param pageUtil
     * @param queryBuilder
     * @return
     * @throws Exception
     */
    List<Map<String,Object>> queryByConditionWithPage(String index,PageUtil pageUtil,QueryBuilder queryBuilder)throws Exception;


    /**
     * 全 and 查询
     *
     * @param index
     * @param params 查询条件  key为字段，value为值
     * @return
     */
    List<Map<String,Object>> mustQueryByCondition(String index,Map<String,Object> params);

    /**
     * 全or查询
     *
     * @param index
     * @param params
     * @return
     */
    List<Map<String,Object>> shouldQueryByCondition(String index,Map<String,Object> params);


    /**
     * 全not 查询
     *
     * @param index
     * @param params
     * @return
     */
    List<Map<String,Object>> mustNotQueryByCondition(String index,Map<String,Object> params);


    /**
     * 复杂查询
     *
     * @param index
     * @param must
     * @param should
     * @param not
     * @return
     */
    List<Map<String,Object>> query(String index,Map<String,Object> must,Map<String,Object> should,Map<String,Object> not);


    /**
     * term查询
     *
     * @param index 索引
     * @param fileId 字段
     * @param value  值
     * @return
     */
    List<Map<String,Object>> termQuery(String index,String fileId,Object value);

    /**
     * terms 查询
     *
     * @param index
     * @param fileId
     * @param values
     * @return
     */
    List<Map<String,Object>> termsQuery(String index,String fileId,Set<Object> values);


    /**
     * 查找那些指定字段中有值 (exists) 或无值 (missing) 的文档
     *
     * @param index
     * @param fileId 字段
     * @return
     */
    List<Map<String,Object>> existsQuery(String index,String fileId);


    /**
     *  多个字段同时匹配文本的查询
     * @param index 索引名
     * @param value 文本
     * @param fileId 字段名称集合
     * @return
     */
    List<Map<String,Object>> multiMatchQuery(String index,Object value,Set<String> fileId);


    /**
     * 模糊查询
     *
     * @param index
     * @param fileId
     * @param value
     * @return
     */
    List<Map<String,Object>>  fuzzyQuery(String index,String fileId,Object value);


    /**
     * 前缀查询 包含指定字段指定前缀
     *
     * @param index
     * @param fileId
     * @param prefix
     * @return
     */
    List<Map<String,Object>>  prefixQuery(String index,String fileId,String prefix);


    /**
     *
     *
     * @param index
     * @param fileId
     * @param from
     * @param to
     * @param includeLower 包括下界
     * @param includeUpper 包括上界
     * @return
     */
    List<Map<String,Object>> rangeQuery(String index,String fileId,Object from,Object to,boolean includeLower,boolean includeUpper);


    /**
     * 通配符查询
     * @param index
     * @param fileId
     * @param wildcard eg：我*你 ?代表单个字符，*代表0个或多个字符
     * @return
     */
    List<Map<String,Object>> wildcardQuery (String index,String fileId,String wildcard);


    /**
     * 正则表达式查询
     *
     * @param index
     * @param fileId
     * @param regexp
     * @return
     */
    List<Map<String,Object>>  regexpQuery(String index,String fileId,String regexp);


    /**
     * 跨度查询 span first
     *
     * @param index
     * @param fileId
     * @param value
     * @param maxEnd Max查询范围的结束位置
     * @return
     */
    List<Map<String,Object>> spanFirstQuery(String index,String fileId,String value,int maxEnd);
}
