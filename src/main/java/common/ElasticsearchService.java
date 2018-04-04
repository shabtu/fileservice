package common;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticsearchService {

    private RestHighLevelClient elasticsearchClient;

    public ElasticsearchService(String hostName){
        elasticsearchClient = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(hostName, 9200, "http"),
                        new HttpHost(hostName, 9201, "http")));
    }

    public RestHighLevelClient getElasticsearchClient() {
        return elasticsearchClient;
    }
}
