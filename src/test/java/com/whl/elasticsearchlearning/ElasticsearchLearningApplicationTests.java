package com.whl.elasticsearchlearning;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.whl.elasticsearchlearning.config.ElasticsearchConfig;
import lombok.Data;
import lombok.ToString;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Map;

@SpringBootTest
class ElasticsearchLearningApplicationTests {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    /**
     * 复杂聚合检索
     * 搜索address中包含mill的所有人的年龄分布以及平均年龄，平均薪资**
     */
    @Test
    public void searchData() throws IOException {
        // 1.创建索引请求
        SearchRequest searchRequest = new SearchRequest();
        // 指定索引
        searchRequest.indices("bank");
        // 1.1 构造检索条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("address","Mill"));
        // 1.2 构造聚合函数
        // 1.2.1 按照年龄分布进行聚合
        SearchSourceBuilder ageAgg = sourceBuilder.aggregation(AggregationBuilders.terms("ageAgg").field("age").size(10));

        // 1.2.2 求平均年龄
        SearchSourceBuilder ageAvg = sourceBuilder.aggregation(AggregationBuilders.avg("ageAvg").field("age"));

        // 1.2.3 求平均薪资
        SearchSourceBuilder balanceAvg = sourceBuilder.aggregation(AggregationBuilders.avg("balanceAvg").field("balance"));
        System.out.println("检索条件"+sourceBuilder);
        // 3.指定DSL，检索条件
        searchRequest.source(sourceBuilder);
        // 2.执行检索
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, ElasticsearchConfig.COMMON_OPTIONS);
        // 3.分析结果
        System.out.println("检索结果 " + searchResponse);
        // 4.检索结果封装为Bean
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            Bank bank = JSON.parseObject(sourceAsString, Bank.class);
            System.out.println("命中文档" + bank);
        }
        // 5.获取聚合结果
        Aggregations aggregations = searchResponse.getAggregations();
        Terms ageAggRes = aggregations.get("ageAgg");
        for (Terms.Bucket bucket : ageAggRes.getBuckets()) {
            String keyAsString = bucket.getKeyAsString();
            System.out.println("年龄分布："+keyAsString+" ==> "+bucket.getDocCount());
        }
        Avg ageAvg1 = aggregations.get("ageAvg");
        System.out.println("平均年龄："+ageAvg1.getValue());
        Avg balanceAvg1 = aggregations.get("balanceAvg");
        System.out.println("平均薪资："+balanceAvg1.getValue());
    }

    /**
     * 一般检索
     * 查询bank索引中state="AK"的文档
     */
    @Test
    public void searchNormalData() throws IOException {
        SearchRequest searchRequest = new SearchRequest("bank");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("state","AK"));
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, ElasticsearchConfig.COMMON_OPTIONS);
        System.out.println("searchResponse = " + searchResponse);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            Bank bank = JSON.parseObject(sourceAsString, Bank.class);
            System.out.println("bank = " + bank);
        }
    }

    /**
     * 测试获取数据
     * 根据id查询数据
     */
    @Test
    public void getData() throws IOException {
        GetRequest getRequest = new GetRequest("users", "1");
        GetResponse getResponse = restHighLevelClient.get(getRequest, ElasticsearchConfig.COMMON_OPTIONS);
        System.out.println("getResponse = " + getResponse);
        Map<String, DocumentField> fields = getResponse.getFields();
        fields.forEach((key,documentField)->{
            System.out.println(key+"=>"+documentField.toString());
        });
    }


    /**
     * 测试存储数据到es
     * 设定了id也可以执行更新操作
     */
    @Test
    public void  indexData() throws IOException {
        IndexRequest indexRequest = new IndexRequest("users");
        indexRequest.id("1");
        User user = new User();
        user.setUsername("lisi");
        user.setAge(25);
        user.setGender("男");
        String jsonString = JSON.toJSONString(user);
        indexRequest.source(jsonString,XContentType.JSON);
        IndexResponse response = restHighLevelClient.index(indexRequest, ElasticsearchConfig.COMMON_OPTIONS);
        System.out.println("response = " + response);
    }

    @Data
    class User{
        private String username;
        private int age;
        private String gender;
    }

    @ToString
    @Data
    static class Bank {

        private int account_number;
        private int balance;
        private String firstname;
        private String lastname;
        private int age;
        private String gender;
        private String address;
        private String employer;
        private String email;
        private String city;
        private String state;
    }

    @Test
    public void contextLoads() {
        System.out.println("restHighLevelClient = " + restHighLevelClient);
    }
}

