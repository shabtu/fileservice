import io.minio.errors.*;
import org.xmlpull.v1.XmlPullParserException;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;


public class Main {


    public static final String MINIO_ENDPOINT = "http://localhost:9000";
    public static final String RMQ_ENDPOINT = "10.12.97.194";
    public static final String ACCESS_KEY = "minio";
    public static final String SECRET_KEY = "minio123";
    public static final String BUCKET_NAME = "images";

    public static void main(String[] args) throws IOException, TimeoutException, InvalidKeyException, NoSuchAlgorithmException, XmlPullParserException, InvalidPortException, InternalException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InvalidEndpointException, RegionConflictException {
        // Create a minioClient with the Minio Server name, Port, Access key and Secret key.

        String fileName = "test.jpg";

        EventReceiver eventReceiver = new EventReceiver(RMQ_ENDPOINT);
        eventReceiver.createConnection();
        eventReceiver.initiateConsumer();

        eventReceiver.run();

        FileService fileService = new FileService(MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY);

        fileService.createBucket(BUCKET_NAME);

        fileService.putObject(BUCKET_NAME, fileName, fileName);


    }
}
