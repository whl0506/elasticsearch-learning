package com.whl.elasticsearchlearning.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchConfig {

    //配置返回RestHighLevelClient客户端
    //参照API地址：https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high.html
    @Bean
    public RestHighLevelClient esClient(){
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.56.133", 9200, "http")));
        return client;
    }
}
