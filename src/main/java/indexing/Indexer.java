package indexing;

import common.FileStorage;
import deblober.AttachmentFile;
import io.minio.errors.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlpull.v1.XmlPullParserException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;



public class Indexer {

    private static final Logger log = LoggerFactory.getLogger(Indexer.class);

    private static final String MINIO_ENDPOINT = "http://localhost:9000";
    private static final String ACCESS_KEY = "minio";
    private static final String SECRET_KEY = "minio123";
    public static final String BUCKET_NAME = "vismaproceedoaplfile";

    private static final String RMQ_ENDPOINT = "localhost";


    public static void main(String[] args) throws IOException, TimeoutException, InvalidKeyException, NoSuchAlgorithmException, XmlPullParserException, InvalidPortException, InternalException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InvalidEndpointException, RegionConflictException {
        // Create a minioClient with the Minio Server name, Port, Access key and Secret key.

        int numberOfThreads = 128, distribution = 0;

        EventReceiver[] eventReceivers = initiateEventReceivers(numberOfThreads);

        FileUploader[] fileUploaders = initiateFileStorages(numberOfThreads);

        File[] filesToInject = new File("downloads").listFiles();

        log.info("Number of files: " + filesToInject.length);

        for (File file : filesToInject){
            FileInputStream fileInputStream = new FileInputStream(file);

            AttachmentFile attachmentFile = createAttachmentFile(file.getName(), fileInputStream);

            log.info(fileUploaders[0].getName());

            fileUploaders[distribution%numberOfThreads].addToBuffer(attachmentFile);
            distribution++;
        }

        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

        for (EventReceiver eventReceiver : eventReceivers)
            executor.execute(eventReceiver);

        for (FileUploader fileUploader: fileUploaders)
            executor.execute(fileUploader);

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

    private static FileUploader[] initiateFileStorages(int numberOfThreads) throws InvalidPortException, InvalidEndpointException, IOException, XmlPullParserException, NoSuchAlgorithmException, InvalidKeyException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InternalException, RegionConflictException {

        log.info("Initiating storage threads..");

        FileUploader[] fileUploaders = new FileUploader[numberOfThreads];

        for (int i = 0; i < fileUploaders.length; i++) {
            fileUploaders[i] = new FileUploader(MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY);

            if (!fileUploaders[i].checkIfBucketExists(BUCKET_NAME))
                fileUploaders[i].createBucket(BUCKET_NAME);

        }

        return fileUploaders;
    }

    private static EventReceiver[] initiateEventReceivers(int numberOfThreads) throws InvalidPortException, InvalidEndpointException, IOException, XmlPullParserException, NoSuchAlgorithmException, InvalidKeyException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InternalException, RegionConflictException, TimeoutException {

        log.info("Initiating storage threads..");

        EventReceiver[] eventReceivers = new EventReceiver[numberOfThreads];

        for (int i = 0; i < eventReceivers.length ; i++) {
            eventReceivers[i] = new EventReceiver(RMQ_ENDPOINT);
            eventReceivers[i].createConnection();
            eventReceivers[i].initiateConsumer();
        }

        return eventReceivers;
    }
}
