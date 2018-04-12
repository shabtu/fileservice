package search;

import common.ElasticsearchService;
import common.FileInfo;
import common.FileStorage;
import deblober.AttachmentFile;
import io.minio.errors.*;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.xmlpull.v1.XmlPullParserException;

import java.io.File;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.Map;

public class Search {
    private static final String ES_ENDPOINT = "localhost";
    private static final String MINIO_ENDPOINT = "http://localhost:9000";
    private static final String ACCESS_KEY = "minio";
    private static final String SECRET_KEY = "minio123";

    public static void main(String[] args) throws IOException, InvalidPortException, InvalidEndpointException, NoSuchAlgorithmException, XmlPullParserException, InvalidKeyException, InsufficientDataException, InvalidArgumentException, InternalException, NoResponseException, ErrorResponseException, InvalidBucketNameException {
        ElasticsearchService elasticsearchService = new ElasticsearchService(ES_ENDPOINT);

        SearchRequest searchRequest = new SearchRequest("invoices");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("bo_sno", 576));
        searchRequest.source(searchSourceBuilder);
        SearchHits hits = elasticsearchService.getElasticsearchClient().search(searchRequest).getHits();


        LinkedList<FileInfo> filePaths = new LinkedList<>();
        hits.forEach(searchHitField ->
                filePaths.add(
                        new FileInfo(
                                Integer.parseInt(searchHitField.getSource().get("sno").toString()),
                                Integer.parseInt(searchHitField.getSource().get("bo_sno").toString()),
                                searchHitField.getSource().get("date").toString(),
                                searchHitField.getSource().get("uid").toString(),
                                searchHitField.getSource().get("name").toString(),
                                searchHitField.getSource().get("bucket").toString()
                        )
                )
        );

        FileStorage fileStorage = new FileStorage(MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY);

        for (FileInfo fileInfo : filePaths){
            System.out.println("Försöker hämta: " + fileInfo.generateFileName());
            fileStorage.getObject(fileInfo.getBucket(), fileInfo.generateFileName(), "retrievedFiles");
        }
    }
}
