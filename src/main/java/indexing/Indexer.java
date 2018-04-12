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
    private static final String BUCKET_NAME = "images";

    private static final String RMQ_ENDPOINT = "localhost";


    public static void main(String[] args) throws IOException, TimeoutException, InvalidKeyException, NoSuchAlgorithmException, XmlPullParserException, InvalidPortException, InternalException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InvalidEndpointException, RegionConflictException {
        // Create a minioClient with the Minio Server name, Port, Access key and Secret key.

        int numberOfThreads = 100, distribution = 0;

        EventReceiver[] eventReceivers = initiateEventReceivers(numberOfThreads);

        FileStorage[] fileStorages = initiateFileStorages(numberOfThreads);

        File[] filesToInject = new File("downloads").listFiles();

        log.info("Number of files: " + filesToInject.length);

        for (File file : filesToInject){
            FileInputStream fileInputStream = new FileInputStream(file);
            //byte[] data = new byte[(int) file.length()];
            //fileInputStream.read(data);


            AttachmentFile attachmentFile = parseFile(file.getName(), fileInputStream);

            log.info(fileStorages[0].getName());

            fileStorages[distribution%numberOfThreads].addToBuffer(attachmentFile);
            distribution++;
        }

        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

        for (EventReceiver eventReceiver : eventReceivers)
            executor.execute(eventReceiver);

        for (FileStorage fileStorage: fileStorages)
            executor.execute(fileStorage);
        //crawlDirectoryAndProcessFiles(filesToInject, executor);

        //runFileStorageThreads(fileStorages);

    }

    private static AttachmentFile parseFile(String file, InputStream fileData){

        String[] fields = file.split("_");

        return new AttachmentFile(Integer.parseInt(fields[0]),
                Integer.parseInt(fields[1]),
                fields[2],
                fields[3],
                fields[4],
                fileData,
                BUCKET_NAME);
    }

    private static void runFileStorageThreads(FileStorage[] fileStorages) {
        for (FileStorage fileStorage : fileStorages) {
            fileStorage.run();
        }
    }

    private static void runEventReceiverThreads(EventReceiver[] eventReceivers) {
        for (EventReceiver eventReceiver : eventReceivers) {
            eventReceiver.run();
        }
    }

    private static FileStorage[] initiateFileStorages(int numberOfThreads) throws InvalidPortException, InvalidEndpointException, IOException, XmlPullParserException, NoSuchAlgorithmException, InvalidKeyException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InternalException, RegionConflictException {

        log.info("Initiating storage threads..");

        FileStorage[] fileStorages = new FileStorage[numberOfThreads];

        for (int i = 0; i < fileStorages.length; i++) {
            fileStorages[i] = new FileStorage(MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY);

            if (!fileStorages[i].checkIfBucketExists(BUCKET_NAME))
                fileStorages[i].createBucket(BUCKET_NAME);

        }

        return fileStorages;
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
