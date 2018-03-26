package search;

import common.ElasticsearchService;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

public class Search {
    public static final String ES_ENDPOINT = "localhost";

    public static void main(String[] args) throws IOException {
        ElasticsearchService elasticsearchService = new ElasticsearchService(ES_ENDPOINT);

        SearchRequest searchRequest = new SearchRequest("invoices");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("uid", "xxxxxxxx-xxxxxxxx-xxxxxxxx-xxxxxxxx"));
        searchRequest.source(searchSourceBuilder);
        SearchHits hits = elasticsearchService.getElasticsearchClient().search(searchRequest).getHits();

    }
}
