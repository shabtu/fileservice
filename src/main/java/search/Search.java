package search;

import common.ElasticsearchService;
import common.FileInfo;
import common.FileStorage;
import io.minio.errors.*;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.QueryBuilders;

import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlpull.v1.XmlPullParserException;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;


public class Search {

    private static final String ES_ENDPOINT = "localhost";
    private static final String MINIO_ENDPOINT = "http://localhost:9000";
    private static final String ACCESS_KEY = "minio";
    private static final String SECRET_KEY = "minio123";
    static final String BUCKET_NAME = "vismaproceedoaplfile";

    private static final Logger log = LoggerFactory.getLogger(Search.class);
    private static int numberOfThreads = 10;

    int numberOfFiles = 1000;
    AtomicInteger searchCounter = new AtomicInteger(0);

    public void runSearch() throws IOException, InvalidPortException, InvalidEndpointException, NoSuchAlgorithmException, XmlPullParserException, InvalidKeyException, InsufficientDataException, InvalidArgumentException, InternalException, NoResponseException, ErrorResponseException, InvalidBucketNameException, RegionConflictException {

        ElasticsearchService elasticsearchService = new ElasticsearchService(ES_ENDPOINT);

        FileSearcher[] fileSearchers = initiateFileDownloaders(numberOfThreads);

        LinkedList<String> filesToIndex = new FileStorage(MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY).listObjects(BUCKET_NAME);
                log.info("Number of files: " + filesToIndex.size());

        LinkedList<FileInfo> filePaths = new LinkedList<>();

        for (String file : filesToIndex) {
            FileInfo targetFile = createAttachmentFile(file);

            SearchRequest searchRequest = new SearchRequest("invoices");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchQuery("sno", targetFile.getSno()));
            searchSourceBuilder.query(QueryBuilders.matchQuery("bo_sno", targetFile.getBo_sno()));
            searchSourceBuilder.query(QueryBuilders.matchQuery("date", targetFile.getCreationDate()));
            searchSourceBuilder.query(QueryBuilders.matchQuery("uid", targetFile.getUniqueId()));
            searchSourceBuilder.query(QueryBuilders.matchQuery("name", targetFile.getName()));
            searchRequest.source(searchSourceBuilder);

            SearchHits hits = elasticsearchService.getElasticsearchClient().search(searchRequest).getHits();


            //Retrieve results from search
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
        }

        int distribution = 0;
        for (FileInfo fileInfo : filePaths) {
            fileSearchers[distribution%numberOfThreads].addToBuffer(fileInfo);
            distribution++;
        }

        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

        for (FileSearcher fileSearcher: fileSearchers)
            executor.execute(fileSearcher);


    }

    private static FileInfo createAttachmentFile(String file){

        String[] fields = file.split("/");

        return new FileInfo(Integer.parseInt(fields[0]),
                Integer.parseInt(fields[1]),
                fields[2],
                fields[3],
                fields[4],
                BUCKET_NAME);
    }

    private FileSearcher[] initiateFileDownloaders(int numberOfThreads) throws InvalidPortException, InvalidEndpointException, IOException, InvalidKeyException, NoSuchAlgorithmException, XmlPullParserException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InternalException, RegionConflictException {

        log.info("Initiating storage threads..");

        FileSearcher[] fileSearchers = new FileSearcher[numberOfThreads];

        for (int i = 0; i < fileSearchers.length; i++) {
            fileSearchers[i] = new FileSearcher(MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY, this);
        }

        return fileSearchers;
    }
}
