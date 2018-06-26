package com.tmg.internship.datacanal.escenter.esengine;


import com.alibaba.fastjson.JSON;
import com.tmg.internship.datacanal.escenter.esengine.model.BaseIndex;
import com.tmg.internship.datacanal.escenter.esengine.model.CreateIndex;
import com.tmg.internship.datacanal.escenter.esengine.model.PutMapping;
import com.tmg.internship.datacanal.escenter.esengine.service.HighLevelClientService;

import com.tmg.internship.datacanal.escenter.trigger.ESMapperCaster;
import com.tmg.utils.JsonUtils;
import com.tmg.utils.http.HttpUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;


import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;

/**
 * @author chensl [cookchensl@gmail.com]
 * @date 2018/5/10 19:00
 * @description
 */
@RunWith(SpringRunner.class)
@SpringBootTest()
public class TestConfig {
/*
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    private static RestHighLevelClient client;

    @PostConstruct
    public void init() {
        client = this.restHighLevelClient;
    }*/

   @Autowired
    private HighLevelClientService highLevelClientService;

    @Test
    public  void test() throws Exception {
        /*SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.aggregation(AggregationBuilders.terms("top_10_states").field("state").size(10));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("social-*");
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest);
        System.out.println(searchResponse.getHits());*/


        /*MainResponse response = client.info();
        ClusterName clusterName = response.getClusterName();
        String clusterUuid = response.getClusterUuid();
        String nodeName = response.getNodeName();
        Version version = response.getVersion();
        Build build = response.getBuild();

        System.out.println(clusterName.toString());
        System.out.println(clusterUuid);
        System.out.println(nodeName);
        System.out.println(version);
        System.out.println(build.toString());
        client.close();
*/
       /* BaseIndex baseIndex=new BaseIndex();
        baseIndex.setIndex("test_index3");
        highLevelClientService.deleteIndex(baseIndex);*/

        /*CreateIndex createIndex=new CreateIndex();
        createIndex.setIndex("test_index4");
       Map<String,Object> settings=new HashMap<>();
       //settings.put("number_of_shards",5);
       // settings.put("number_of_replicas",1);
       settings.put("analysis","{\"analyzer\":{\"token\":{\"tokenizer\":\"standard\"}}}");
        System.out.println(settings);
        System.out.println(JsonUtils.toJSONString(settings));
        createIndex.setSettings(settings);


        highLevelClientService.createIndex(createIndex);
        System.out.println("success !!!!");*/
       // ClientInterface clientUtil = ElasticSearchHelper.getRestClientUtil();
        //验证环境,获取es状态
        //String response = clientUtil.executeHttp("test222/_mapping?pretty",ClientInterface.HTTP_GET);
       /* Map<String,Object> map=new HashMap<>();
        map.put("name","csl");
        map.put("age",24);

        String response =clientUtil.addDateDocument("test_index6","test_index6",map,"refresh=true");
        System.out.println(response);
*/



/*
        PutMapping putMapping=new PutMapping();

        CreateIndex createIndex=new CreateIndex();
        createIndex.setIndex("test228");
        createIndex.setType("doc");
        createIndex.setAlias("test228$doc");

        putMapping.setIndex("test228");
        putMapping.setType("doc");
        Map<String,Object> mapping=new HashMap<>();
        Map<String,Object> properties=new HashMap<>();
        Map<String,Object> title=new HashMap<>();
        Map<String,Object> source=new HashMap<>();
        title.put("type","keyword");
        properties.put("tag",title);
        mapping.put("properties",properties);
        source.put("doc",mapping);
        //mapping.put("doc","{\"properties\":{\"tag\":{\"type\":\"keyword\"},\"title\":{\"type\":\"text\",\"analyzer\":\"english\"}}}");
        putMapping.setSource(source);

        createIndex.setMappings(source);*/



        //analysis","{\"analyzer\":{\"token\":{\"tokenizer\":\"standard\"}}}

       /* Map<String,Object> settings=new HashMap<>();
        Map<String,Object> analysis=new HashMap<>();
        Map<String,Object> analyzer=new HashMap<>();
        Map<String,Object> token=new HashMap<>();

        token.put("tokenizer","standard");
        analyzer.put("token",token);
        analysis.put("analyzer",analyzer);
        settings.put("analysis",analysis);
        createIndex.setSettings(settings);


       highLevelClientService.createIndex(createIndex);*/


       // System.out.println(highLevelClientService.putMapping(putMapping));
        Set<String> set= new HashSet<>();
        set.add("create_user_id");
        set.add("email");
        System.out.println(ESMapperCaster.getFieldMapping("xiyou_prod@t_sys_user",set));




    }

}
