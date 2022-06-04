package com.kafka.aws.config;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class ElasticSearchConfig {
    @Bean(destroyMethod = "close")
//    @Scope("prototype")
    public RestHighLevelClient createClient() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("kevin","Lio87979!.!"));

        RestHighLevelClient restClient = new RestHighLevelClient(RestClient.builder(new HttpHost(
                "search-messaging-elasticsearch-wixnzdcsd2wgihinrcbhvrnipa.us-east-2.es.amazonaws.com",
                443, "https")).setHttpClientConfigCallback((httpAsyncClientBuilder) ->
                httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)));

        return restClient;
    }
}
