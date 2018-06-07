package search;

import common.ElasticsearchService;
import common.FileInfo;
import common.FileStorage;
import io.minio.errors.*;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.QueryBuilders;

import org.elasticsearch.search.SearchHit;
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

    /* Credentials for accessing Minio and Elasticsearch*/
    private static final String MINIO_ENDPOINT = "http://localhost:9000";
    private static final String ACCESS_KEY = "minio";
    private static final String SECRET_KEY = "minio123";
    static final String BUCKET_NAME = "vismaproceedoaplfile";
    private static final String ES_ENDPOINT = "10.12.97.63";

    private static final Logger log = LoggerFactory.getLogger(Search.class);
    private final LinkedList<String> filesToFind;
    private LinkedList<FileInfo> filePaths;
    private int numberOfThreads;

    AtomicInteger searchCounter = new AtomicInteger(0);

    private FileSearcher[] fileSearchers;
    public int numberOfFiles;

    public Search(int numberOfThreads) throws InvalidPortException, InvalidEndpointException, IOException, InsufficientDataException, NoSuchAlgorithmException, XmlPullParserException, NoResponseException, InternalException, InvalidBucketNameException, InvalidKeyException, ErrorResponseException, RegionConflictException {
        this.numberOfThreads = numberOfThreads;

        fileSearchers = initiateFileSearchers(numberOfThreads);

        /* Get all files from the object storage to search for them in Elasticsearch and Minio */
        filesToFind = new FileStorage(MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY).listObjects(BUCKET_NAME);

        filePaths = new LinkedList<>();

        /* Find files indexes on the Elasticsearch server*/
        filePaths = getFileIndexes(filesToFind);
    }

    public long runSearch(int numberOfFiles) {

        this.numberOfFiles = numberOfFiles;

        searchCounter.set(0);

        /* Distribute file paths among the threads to parallellize it*/
        distributeSearchAmongThreads(filePaths);

        /* Start the threads */
        runSearchers();

        long searchStartTime = System.nanoTime();
        /*Wait until all files are found*/
        while (searchCounter.intValue() < numberOfFiles) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


        long searchStopTime = System.nanoTime();


        long searchTime = (searchStopTime-searchStartTime)/1000000;

        return searchTime;


    }


    public LinkedList<FileInfo> getFileIndexes(LinkedList<String> filesToFind) throws IOException {

        ElasticsearchService elasticsearchService = new ElasticsearchService(ES_ENDPOINT);

        for (String file : filesToFind) {
            FileInfo targetFile = createAttachmentFile(file);

            SearchRequest searchRequest = new SearchRequest("invoices");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchQuery("sno", targetFile.getSno()));
            searchSourceBuilder.query(QueryBuilders.matchQuery("bo_sno", targetFile.getBo_sno()));
            searchSourceBuilder.query(QueryBuilders.matchQuery("date", targetFile.getCreationDate()));
            searchSourceBuilder.query(QueryBuilders.matchQuery("uid", targetFile.getUniqueId()));
            searchSourceBuilder.query(QueryBuilders.matchQuery("name", targetFile.getName()));
            searchRequest.source(searchSourceBuilder);

            SearchHit[] hits = elasticsearchService.getElasticsearchClient().search(searchRequest).getHits().getHits();

            //Retrieve results from search

            filePaths.add(
                    new FileInfo(
                            Integer.parseInt(hits[0].getSource().get("sno").toString()),
                            Integer.parseInt(hits[0].getSource().get("bo_sno").toString()),
                            hits[0].getSource().get("date").toString(),
                            hits[0].getSource().get("uid").toString(),
                            hits[0].getSource().get("name").toString(),
                            hits[0].getSource().get("bucket").toString()
                    )
            );
        }
        return filePaths;
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

    private void distributeSearchAmongThreads(LinkedList<FileInfo> filePaths) {

        int distribution = 0;
        //for (FileInfo fileInfo : filePaths) {
        for (int i = 0; i < numberOfFiles; i++) {

            if (i == 4095) {
                fileSearchers[distribution % numberOfThreads].addToBuffer(filePaths.get(i - 1));
                continue;
            }

            fileSearchers[distribution%numberOfThreads].addToBuffer(filePaths.get(i));
            distribution++;
        }
    }


    private FileSearcher[] initiateFileSearchers(int numberOfThreads) throws InvalidPortException, InvalidEndpointException, IOException, InvalidKeyException, NoSuchAlgorithmException, XmlPullParserException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InternalException, RegionConflictException {

        FileSearcher[] fileSearchers = new FileSearcher[numberOfThreads];

        for (int i = 0; i < fileSearchers.length; i++) {
            fileSearchers[i] = new FileSearcher(MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY, this);
        }

        return fileSearchers;
    }

    private void runSearchers(){

        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

        for (FileSearcher fileSearcher: fileSearchers)
            executor.execute(fileSearcher);
    }
}
