package search;

import common.ElasticsearchService;
import common.FileInfo;
import common.FileStorage;
import deblober.AttachmentFile;
import io.minio.errors.*;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlpull.v1.XmlPullParserException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;


public class Search {
    private static final String ES_ENDPOINT = "localhost";
    private static final String MINIO_ENDPOINT = "http://localhost:9000";
    private static final String ACCESS_KEY = "minio";
    private static final String SECRET_KEY = "minio123";
    private static final String BUCKET_NAME = "images";

    private static final Logger log = LoggerFactory.getLogger(Search.class);

    public static void main(String[] args) throws IOException, InvalidPortException, InvalidEndpointException, NoSuchAlgorithmException, XmlPullParserException, InvalidKeyException, InsufficientDataException, InvalidArgumentException, InternalException, NoResponseException, ErrorResponseException, InvalidBucketNameException {
        ElasticsearchService elasticsearchService = new ElasticsearchService(ES_ENDPOINT);

        File[] filesToInject = new File("downloads").listFiles();

        log.info("Number of files: " + filesToInject.length);

        for (File file : filesToInject) {
            FileInputStream fileInputStream = new FileInputStream(file);
            AttachmentFile attachmentFile = createAttachmentFile(file.getName(), fileInputStream);

            /*SearchRequestBuilder srb1 = elasticsearchService.getElasticsearchClient()
                    .prepareSearch() .setQuery(QueryBuilders.queryStringQuery("elasticsearch")).setSize(1);
            SearchRequestBuilder srb2 = client
                    .prepareSearch().setQuery(QueryBuilders.matchQuery("name", "kimchy")).setSize(1);*/


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

            for (FileInfo fileInfo : filePaths) {
                System.out.println("Försöker hämta: " + fileInfo.generateFileNameWithDirectories());
                fileStorage.getObject(fileInfo.getBucket(), fileInfo.generateFileNameWithDirectories(), "retrievedFiles");
            }
        }
    }

    private static AttachmentFile createAttachmentFile(String file, InputStream fileData){

        String[] fields = file.split("_");

        return new AttachmentFile(Integer.parseInt(fields[0]),
                Integer.parseInt(fields[1]),
                fields[2],
                fields[3],
                fields[4],
                fileData,
                BUCKET_NAME);
    }
}
