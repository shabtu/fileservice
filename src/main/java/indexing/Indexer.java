package indexing;

import deblober.AttachmentFile;

import io.minio.errors.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.xmlpull.v1.XmlPullParserException;

import java.io.IOException;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;


public class Indexer {

    private static final Logger log = LoggerFactory.getLogger(Indexer.class);

    public LongAdder getIndexCounter() {
        return indexCounter;
    }


    public LongAdder indexCounter = new LongAdder();

    private static final String MINIO_ENDPOINT = "http://localhost:9000";
    private static final String ACCESS_KEY = "minio";
    private static final String SECRET_KEY = "minio123";
    public static final String BUCKET_NAME = "vismaproceedoaplfile";

    private static final String RMQ_ENDPOINT = "localhost";

    private int numberOfThreads = 100, distribution = 0;

    private EventReceiver[] eventReceivers;
    FileUploader[] fileUploaders;

    public Indexer() throws IOException, TimeoutException, InvalidKeyException, NoSuchAlgorithmException, RegionConflictException, XmlPullParserException, InvalidPortException, InternalException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InvalidEndpointException, ErrorResponseException {
        eventReceivers = initiateEventReceivers(numberOfThreads);

        fileUploaders = initiateFileStorages(numberOfThreads);
    }

    public void index(LinkedList<AttachmentFile> files) {
        // Create a minioClient with the Minio Server name, Port, Access key and Secret key.

        log.info("Number of files: " + files.size());


        distributeFilesToUploaders(files);

        runUploadersAndReceivers();


        while (indexCounter.intValue() < 1000) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("DONE!!!!");

    }

    private void runUploadersAndReceivers() {

        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads*2);

        for (EventReceiver eventReceiver : eventReceivers)
            executor.execute(eventReceiver);


        for (FileUploader fileUploader: fileUploaders)
            executor.execute(fileUploader);
    }

    private void distributeFilesToUploaders(LinkedList<AttachmentFile> files) {
        for (AttachmentFile attachmentFile : files){

            fileUploaders[distribution%numberOfThreads].addToBuffer(attachmentFile);
            distribution++;
        }

    }

    /*private static AttachmentFile createAttachmentFile(String file, InputStream fileData){

        String[] fields = file.split("_");

        return new AttachmentFile(Integer.parseInt(fields[0]),
                Integer.parseInt(fields[1]),
                fields[2],
                fields[3],
                fields[4],
                fileData,
                BUCKET_NAME);
    }*/

    private FileUploader[] initiateFileStorages(int numberOfThreads) throws InvalidPortException, InvalidEndpointException, IOException, XmlPullParserException, NoSuchAlgorithmException, InvalidKeyException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InternalException, RegionConflictException {

        log.info("Initiating storage threads..");

        FileUploader[] fileUploaders = new FileUploader[numberOfThreads];

        for (int i = 0; i < fileUploaders.length; i++) {
            fileUploaders[i] = new FileUploader(MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY);

            if (!fileUploaders[i].checkIfBucketExists(BUCKET_NAME))
                fileUploaders[i].createBucket(BUCKET_NAME);

        }

        return fileUploaders;
    }

    private EventReceiver[] initiateEventReceivers(int numberOfThreads) throws IOException, TimeoutException {

        log.info("Initiating storage threads..");

        EventReceiver[] eventReceivers = new EventReceiver[numberOfThreads];

        for (int i = 0; i < eventReceivers.length ; i++) {
            eventReceivers[i] = new EventReceiver(RMQ_ENDPOINT, this);
            eventReceivers[i].createConnection();
            eventReceivers[i].initiateConsumer();
        }

        return eventReceivers;
    }
}
