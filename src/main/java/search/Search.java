package search;

import common.ElasticsearchService;
import common.FileInfo;
import common.FileStorage;
import deblober.AttachmentFile;
import io.minio.errors.*;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchAction;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class Search {
    private static final String ES_ENDPOINT = "localhost";
    private static final String MINIO_ENDPOINT = "http://localhost:9000";
    private static final String ACCESS_KEY = "minio";
    private static final String SECRET_KEY = "minio123";
    private static final String BUCKET_NAME = "images";

    private static final Logger log = LoggerFactory.getLogger(Search.class);
    private static int i;

    public static void main(String[] args) throws IOException, InvalidPortException, InvalidEndpointException, NoSuchAlgorithmException, XmlPullParserException, InvalidKeyException, InsufficientDataException, InvalidArgumentException, InternalException, NoResponseException, ErrorResponseException, InvalidBucketNameException, RegionConflictException {
        int numberOfThreads = 2;

        ElasticsearchService elasticsearchService = new ElasticsearchService(ES_ENDPOINT);

        File[] filesToInject = new File("downloads").listFiles();

        FileDownloader[] fileDownloaders = initiateFileStorages(numberOfThreads);

        log.info("Number of files: " + filesToInject.length);

        LinkedList<FileInfo> filePaths = new LinkedList<>();

        for (File file : filesToInject) {
            System.out.println("File " + i++);
            FileInfo targetFile = createAttachmentFile(file.getName());

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
            fileDownloaders[distribution%numberOfThreads].addToBuffer(fileInfo);
            distribution++;
        }

        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

        for (FileDownloader fileDownloader: fileDownloaders)
            executor.execute(fileDownloader);
    }

    private static FileInfo createAttachmentFile(String file){

        String[] fields = file.split("_");

        return new FileInfo(Integer.parseInt(fields[0]),
                Integer.parseInt(fields[1]),
                fields[2],
                fields[3],
                fields[4],
                BUCKET_NAME);
    }

    private static FileDownloader[] initiateFileStorages(int numberOfThreads) throws InvalidPortException, InvalidEndpointException, IOException, XmlPullParserException, NoSuchAlgorithmException, InvalidKeyException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InternalException, RegionConflictException {

        log.info("Initiating storage threads..");

        FileDownloader[] fileDownloaders = new FileDownloader[numberOfThreads];

        for (int i = 0; i < fileDownloaders.length; i++) {
            fileDownloaders[i] = new FileDownloader(MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY);

            if (!fileDownloaders[i].checkIfBucketExists(BUCKET_NAME))
                fileDownloaders[i].createBucket(BUCKET_NAME);

        }

        return fileDownloaders;
    }
}
