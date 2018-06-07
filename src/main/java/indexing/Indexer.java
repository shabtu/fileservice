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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;


public class Indexer {

    private static final Logger log = LoggerFactory.getLogger(Indexer.class);

    /*Number of files to be indexed*/
    private int numberOfFiles;

    public AtomicInteger indexCounter = new AtomicInteger();

    /*/Credentials for accessing Minio*/
    private static final String MINIO_ENDPOINT = "http://localhost:9000";
    private static final String ACCESS_KEY = "minio";
    private static final String SECRET_KEY = "minio123";
    public static final String BUCKET_NAME = "vismaproceedoaplfile";

    /*RabbitMQ endpoint*/
    private static final String RMQ_ENDPOINT = "localhost";

    /*Number of threads each for the file uploaders and the event receivers,
     distributed using the "distribution" counter*/
    private int numberOfThreads, distribution = 0;

    /*Event receivers and file uploaders that are to be used*/
    private EventReceiver[] eventReceivers;
    private FileUploader[] fileUploaders;
    private static LinkedList<AttachmentFile> files = new LinkedList<>();

    public Indexer(int numberOfThreads) throws IOException, TimeoutException, InvalidKeyException, NoSuchAlgorithmException, RegionConflictException, XmlPullParserException, InvalidPortException, InternalException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InvalidEndpointException, ErrorResponseException {

        /*Event receivers and file uploaders are initiated*/
        eventReceivers = initiateEventReceivers(numberOfThreads);
        fileUploaders = initiateFileUploaders(numberOfThreads);

        this.numberOfThreads = numberOfThreads;
    }

    public long index(List<AttachmentFile> files) {

        /*Distribute upload tasks among file uploaders*/
        distributeFilesToUploaders(files);

        /*Start the threads*/
        runUploadersAndReceivers();

        numberOfFiles = files.size();
        indexCounter.set(0);
        long indexStartTime = System.nanoTime();

        /*Busy-waiting until all files are indexed*/
        while (indexCounter.intValue() < files.size()) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long indexStopTime = System.nanoTime();

        files = null;

        return (indexStopTime-indexStartTime)/1000000000;


    }

    private void runUploadersAndReceivers() {

        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads*2);

        for (EventReceiver eventReceiver : eventReceivers)
            executor.execute(eventReceiver);


        for (FileUploader fileUploader: fileUploaders)
            executor.execute(fileUploader);

    }

    private void distributeFilesToUploaders(List<AttachmentFile> files) {
        for (AttachmentFile attachmentFile : files){

            fileUploaders[distribution%numberOfThreads].addToBuffer(attachmentFile);
            distribution++;
        }

    }

    private FileUploader[] initiateFileUploaders(int numberOfThreads) throws InvalidPortException, InvalidEndpointException, IOException, XmlPullParserException, NoSuchAlgorithmException, InvalidKeyException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InternalException, RegionConflictException {

        FileUploader[] fileUploaders = new FileUploader[numberOfThreads];

        for (int i = 0; i < fileUploaders.length; i++) {

            fileUploaders[i] = new FileUploader(MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY);

            if (!fileUploaders[i].checkIfBucketExists(BUCKET_NAME))
                fileUploaders[i].createBucket(BUCKET_NAME);

        }

        return fileUploaders;
    }

    private EventReceiver[] initiateEventReceivers(int numberOfThreads) throws IOException, TimeoutException {

        EventReceiver[] eventReceivers = new EventReceiver[numberOfThreads];

        for (int i = 0; i < eventReceivers.length ; i++) {
            eventReceivers[i] = new EventReceiver(RMQ_ENDPOINT, this);
            eventReceivers[i].createConnection();
            eventReceivers[i].initiateConsumer();
        }

        return eventReceivers;
    }

    public int getNumberOfFiles() {
        return numberOfFiles;
    }
}
