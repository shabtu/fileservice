package indexing;

import common.FileStorage;
import io.minio.errors.*;
import org.xmlpull.v1.XmlPullParserException;

import java.io.File;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeoutException;



public class Indexer {


    private static final String MINIO_ENDPOINT = "http://localhost:9000";
    private static final String RMQ_ENDPOINT = "localhost";
    private static final String ACCESS_KEY = "minio";
    private static final String SECRET_KEY = "minio123";
    private static final String BUCKET_NAME = "images";

    public static ArrayBlockingQueue<File> files;

    public static void main(String[] args) throws IOException, TimeoutException, InvalidKeyException, NoSuchAlgorithmException, XmlPullParserException, InvalidPortException, InternalException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InvalidEndpointException, RegionConflictException {
        // Create a minioClient with the Minio Server name, Port, Access key and Secret key.

        String fileName = "test.jpg";

        EventReceiver[] eventReceivers = new EventReceiver[1];

        for (EventReceiver eventReceiver : eventReceivers) {
            eventReceiver = new EventReceiver(RMQ_ENDPOINT);
            eventReceiver.createConnection();
            eventReceiver.initiateConsumer();
            eventReceiver.run();
        }

        FileStorage[] fileStorages = new FileStorage[1];

        File[] filesToInject = new File("downloads/").listFiles();

        int partitionSize = filesToInject.length/fileStorages.length;
        int partitioned = 0;

       /* files = new ArrayBlockingQueue<>(filesToInject.length);

        for (File file : filesToInject)
            files.add(file);
*/
        System.out.println("Number or files: " + filesToInject.length + "\n With partition size: " + partitionSize);

        for (int i = 0; i < fileStorages.length; i++) {
            fileStorages[i] = new FileStorage(MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY);
            for (int j = partitioned; j < (partitioned + partitionSize); j++) {
                fileStorages[i].addToBuffer(filesToInject[j]);
            }
            partitioned += partitionSize;
        }
        for (FileStorage fileStorage : fileStorages) {

            if (!fileStorage.checkIfBucketExists(BUCKET_NAME))
                fileStorage.createBucket(BUCKET_NAME);

            fileStorage.run();
        }
    }

    public static ArrayBlockingQueue<File> getFiles() {
        return files;
    }
}
