
import io.minio.errors.*;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.InvalidKeyException;
import org.xmlpull.v1.XmlPullParserException;
import io.minio.MinioClient;

import java.io.*;
import java.util.*;


public class FileService {

    private final MinioClient minioClient;

    public FileService(String endpoint, String accessKey, String secretKey) throws InvalidPortException, InvalidEndpointException, IOException, XmlPullParserException, NoSuchAlgorithmException, RegionConflictException, InvalidKeyException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InternalException {

        minioClient = createMinioClient(endpoint, accessKey, secretKey);
    }


    public static MinioClient createMinioClient(String endpoint, String accessKey, String secretKey) throws InvalidPortException, InvalidEndpointException {
        return new MinioClient(endpoint, accessKey, secretKey);

    }

    public void createBucket(String bucketName) throws IOException, InvalidKeyException, NoSuchAlgorithmException, InsufficientDataException, InternalException, NoResponseException, InvalidBucketNameException, XmlPullParserException, ErrorResponseException, RegionConflictException {
        // Check if the bucket already exists.
        if(checkIfBucketExists(bucketName)) {
            System.out.println("Bucket already exists.");
        } else {
            // Make a new bucket to hold files.
            minioClient.makeBucket(bucketName);
        }
    }

    public boolean checkIfBucketExists(String bucketName) throws IOException, InvalidKeyException, NoSuchAlgorithmException, InsufficientDataException, InternalException, NoResponseException, InvalidBucketNameException, XmlPullParserException, ErrorResponseException {
        return minioClient.bucketExists(bucketName);
    }
    public void putObject(String bucketName, String objectName, String fileName) throws XmlPullParserException, NoSuchAlgorithmException, InvalidKeyException, IOException {
        try{
            // Upload the zip file to the bucket with putObject
            minioClient.putObject(bucketName, fileName, fileName);
            System.out.println("newFile is successfully uploaded as asiaphotos.zip to `asiatrip` bucket.");
        } catch (MinioException e) {
            System.out.println("Error occurred: " + e);
        }

    }
}
