import common.FileStorage;
import io.minio.errors.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.xmlpull.v1.XmlPullParserException;

import java.io.File;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileStorageTest {

    final String MINIO_ENDPOINT = "http://localhost:9000";
    final String ACCESS_KEY = "minio";
    final String SECRET_KEY = "minio123";
    final String BUCKET_NAME = "images";

    static FileStorage fileStorage;

    FileStorageTest() throws IOException, InvalidKeyException, NoSuchAlgorithmException, XmlPullParserException, InvalidPortException, InternalException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InvalidEndpointException, RegionConflictException {

    }


    @BeforeEach
    void init() throws IOException, InvalidKeyException, NoSuchAlgorithmException, XmlPullParserException, InvalidPortException, InternalException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InvalidEndpointException, RegionConflictException {

        fileStorage = new FileStorage(MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY, 1);
        fileStorage.createMinioClient(MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY);
        fileStorage.createBucket(BUCKET_NAME);
        fileStorage.putObject(BUCKET_NAME, "test1.jpg", "test.jpg");
        fileStorage.putObject(BUCKET_NAME, "test2.pdf", "test.pdf");
        fileStorage.putObject(BUCKET_NAME, "test3.jpg", "test.jpg");
    }

    @Test
    void checkBucketTest() throws IOException, InvalidKeyException, NoSuchAlgorithmException, XmlPullParserException, InvalidPortException, InternalException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InvalidEndpointException, RegionConflictException {
        assertEquals(true, fileStorage.checkIfBucketExists(BUCKET_NAME));
    }

    @Test
    void putConfirmationTest() throws IOException, XmlPullParserException, NoSuchAlgorithmException, InvalidKeyException, InvalidArgumentException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InternalException {
        fileStorage.getObject(BUCKET_NAME, "test1.jpg", "retrievedFiles");
        fileStorage.getObject(BUCKET_NAME, "test2.pdf", "retrievedFiles");
        fileStorage.getObject(BUCKET_NAME, "test3.jpg", "retrievedFiles");

        File[] files = {new File("downloads/test1.jpg"), new File("downloads/test2.pdf"), new File("downloads/test3.jpg")};

        assertEquals(true, files[0].exists());
        assertEquals(true, files[1].exists());
        assertEquals(true, files[2].exists());
    }
}
