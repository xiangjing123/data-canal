package com.tmg.internship.datacanal.escenter.esengine;

import com.tmg.internship.datacanal.escenter.esengine.service.TransportClientService;
import com.tmg.utils.JsonUtils;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author chensl [cookchensl@gmail.com]
 * @date 2018/5/31 11:41
 * @description
 * @since 2.8.1
 */

@RunWith(SpringRunner.class)
@SpringBootTest()
public class TransportClientTest {
    @Autowired
    private TransportClientService clientService;

    @Test
    public  void test() throws ExecutionException, InterruptedException {
       // System.out.println(clientService.isExists("cars"));
       // System.out.println(clientService.closeIndex("cars"));
      //  System.out.println(clientService.openIndex("cars"));
       // System.out.println(clientService.deleteIndex("csl_test"));
       // System.out.println(clientService.createIndex("csl_test","csl$test"));

       /* //测试createIndexWithSetting
        Map<String,Object> settings=new HashMap<>();
        Map<String,Object> analysis=new HashMap<>();
        Map<String,Object> analyzer=new HashMap<>();
        Map<String,Object> token=new HashMap<>();
        token.put("tokenizer","standard");
        analyzer.put("token",token);
        analysis.put("analyzer",analyzer);
        settings.put("analysis",analysis);
        System.out.println(clientService.createIndexWithSetting("csl_test1","csl$test1",settings));*/

       /*//测试updateSettings
        Map<String,Object> settings=new HashMap<>();
        Map<String,Object> analysis=new HashMap<>();
        Map<String,Object> analyzer=new HashMap<>();
        Map<String,Object> token=new HashMap<>();
        token.put("tokenizer","keyword");
        analyzer.put("token",token);
        analysis.put("analyzer",analyzer);
        settings.put("analysis",analysis);
        clientService.closeIndex("csl_test1");
        clientService.updateSettings("csl_test1",settings);
        clientService.openIndex("csl_test1");*/

       /* //测试createIndexWitheMapping
        Map<String,Object> mapping=new HashMap<>();
        Map<String,Object> properties=new HashMap<>();
        Map<String,Object> title=new HashMap<>();
        title.put("type","keyword");
        properties.put("tag",title);
        mapping.put("properties",properties);
        clientService.createIndexWitheMapping("csl_test2","doc","csl$test",mapping);*/

        /*//测试putMapping
        Map<String,Object> mapping=new HashMap<>();
        Map<String,Object> properties=new HashMap<>();
        Map<String,Object> title=new HashMap<>();
        title.put("type","keyword");
        properties.put("age",title);
        mapping.put("properties",properties);
        clientService.putMapping("csl_test2","doc",mapping);*/

       /* //测试createIndexWithSettingAndMapping
        Map<String,Object> settings=new HashMap<>();
        Map<String,Object> analysis=new HashMap<>();
        Map<String,Object> analyzer=new HashMap<>();
        Map<String,Object> token=new HashMap<>();
        token.put("tokenizer","keyword");
        analyzer.put("token",token);
        analysis.put("analyzer",analyzer);
        settings.put("analysis",analysis);

        Map<String,Object> mapping=new HashMap<>();
        Map<String,Object> properties=new HashMap<>();
        Map<String,Object> title=new HashMap<>();
        title.put("type","keyword");
        properties.put("age",title);
        mapping.put("properties",properties);
        clientService.createIndexWithSettingAndMapping("csl_test3","doc","csl$test3",settings,mapping);*/

        /*//测试添加数据
        Map<String,Object> doc=new HashMap<>();
        doc.put("age",24);
        clientService.postData("csl_test3","doc","1",doc);*/

       /* //测试根据id获取数据  返回{age=24}的Map
        System.out.println(clientService.getSourceById("csl_test3","doc","1","age"));*/

      /* //测试修改数据
        Map<String,Object> doc=new HashMap<>();
        doc.put("age",18);
        clientService.update("csl_test3","doc","1",doc);*/

      /*//测试updateByUpsert
        Map<String,Object> doc=new HashMap<>();
        doc.put("age",19);
        doc.put("name","小明");
        clientService.updateByUpsert("csl_test3","doc","1",doc);*/


       /*//测试批量插入数据
        Map<String,Object> doc1=new HashMap<>();
        doc1.put("age",22);
        doc1.put("name","张无忌");

        Map<String,Object> doc2=new HashMap<>();
        doc2.put("age",23);
        doc2.put("name","杨过");
        List<Map<String, Object>> docs=new ArrayList<>();
        docs.add(doc1);
        docs.add(doc2);

        clientService.bulkInsertData("csl_test3","doc",docs);*/


       /*//测试删除数据
        System.out.println(clientService.deleteDocument("csl_test3","doc","1"));
        System.out.println(clientService.deleteDocument("csl_test3","doc","WAoU02MBAv8cGWsU6NL6"));
        QueryBuilder queryBuilder = QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("age",22));
        System.out.println(clientService.deleteByQuery("csl_test3",queryBuilder));*/


        QueryBuilder queryBuilder = QueryBuilders.termQuery("_id", "XQoc02MBAv8cGWsUpNLU");
        Map<String,Object> params=new HashMap<>();
        params.put("name","张无忌2");
        clientService.updateByQuery("csl_test3",0, params,queryBuilder);

    }

}
