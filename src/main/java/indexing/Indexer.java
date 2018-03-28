package indexing;

import common.FileStorage;
import io.minio.errors.*;
import org.xmlpull.v1.XmlPullParserException;

import java.io.File;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;


public class Indexer {


    private static final String MINIO_ENDPOINT = "http://localhost:9000";
    private static final String RMQ_ENDPOINT = "localhost";
    private static final String ACCESS_KEY = "minio";
    private static final String SECRET_KEY = "minio123";
    private static final String BUCKET_NAME = "images";

    public static void main(String[] args) throws IOException, TimeoutException, InvalidKeyException, NoSuchAlgorithmException, XmlPullParserException, InvalidPortException, InternalException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InvalidEndpointException, RegionConflictException {
        // Create a minioClient with the Minio Server name, Port, Access key and Secret key.

        String fileName = "test.jpg";

        EventReceiver eventReceiver = new EventReceiver(RMQ_ENDPOINT);
        eventReceiver.createConnection();
        eventReceiver.initiateConsumer();

        eventReceiver.run();

        FileStorage fileStorage = new FileStorage(MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY);

        if (!fileStorage.checkIfBucketExists(BUCKET_NAME))
            fileStorage.createBucket(BUCKET_NAME);

        File[] filesToInject = new File("downloads/").listFiles();

        try {
            for (File file : filesToInject)
                fileStorage.putObject(BUCKET_NAME, file.getName(), file.getAbsolutePath());
        } catch (NullPointerException e){
            System.out.println("No files in directory");
        }

    }
}
