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

    /* Credentials for accessing Minio and Elasticsearch*/
    private static final String ES_ENDPOINT = "localhost";
    private static final String MINIO_ENDPOINT = "http://localhost:9000";
    private static final String ACCESS_KEY = "minio";
    private static final String SECRET_KEY = "minio123";
    static final String BUCKET_NAME = "vismaproceedoaplfile";

    private static final Logger log = LoggerFactory.getLogger(Search.class);
    private static int numberOfThreads = 10;


    public int numberOfFiles;
    AtomicInteger searchCounter = new AtomicInteger(0);


    private FileSearcher[] fileSearchers;

    public Search(int numberOfFiles){
        this.numberOfFiles = numberOfFiles;
    }

    public void runSearch() throws IOException, InvalidPortException, InvalidEndpointException, NoSuchAlgorithmException, XmlPullParserException, InvalidKeyException, InsufficientDataException, InternalException, NoResponseException, ErrorResponseException, InvalidBucketNameException, RegionConflictException {



        /* Get all files from the object storage to search for them in Elasticsearch and Minio */
        LinkedList<String> filesToFind = new FileStorage(MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY).listObjects(BUCKET_NAME);

        LinkedList<FileInfo> filePaths = new LinkedList<>();

        log.info("Number of files: " + filesToFind.size());

        /* Find files indexes on the Elasticsearch server*/
        filePaths = getFileIndexes(filePaths, filesToFind);

        /* Distribute file paths among the threads to parallellize it*/
        distributeSearchAmongThreads(filePaths);

        /* Start the threads */
        runSearchers();

        /*Wait until all files are found*/
        while (searchCounter.intValue() < numberOfFiles) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("DONE!");

    }

    private LinkedList<FileInfo> getFileIndexes(LinkedList<FileInfo> filePaths, LinkedList<String> filesToFind) throws IOException {

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

        return filePaths;
    }

    private void distributeSearchAmongThreads(LinkedList<FileInfo> filePaths) throws IOException, XmlPullParserException, NoSuchAlgorithmException, RegionConflictException, InvalidKeyException, InvalidPortException, InternalException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InvalidEndpointException, ErrorResponseException {

        fileSearchers = initiateFileSearchers(numberOfFiles);

        int distribution = 0;
        for (FileInfo fileInfo : filePaths) {
            fileSearchers[distribution%numberOfThreads].addToBuffer(fileInfo);
            distribution++;
        }
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

    private FileSearcher[] initiateFileSearchers(int numberOfThreads) throws InvalidPortException, InvalidEndpointException, IOException, InvalidKeyException, NoSuchAlgorithmException, XmlPullParserException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InternalException, RegionConflictException {

        log.info("Initiating storage threads..");

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
