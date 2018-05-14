package common;

import deblober.AttachmentFile;
import io.minio.Result;
import io.minio.errors.*;

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.security.InvalidKeyException;
import java.util.LinkedList;

import io.minio.messages.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlpull.v1.XmlPullParserException;
import io.minio.MinioClient;



public class FileStorage extends Thread {


    private static final Logger log = LoggerFactory.getLogger(FileStorage.class);

    protected final MinioClient minioClient;

    public FileStorage(String endpoint, String accessKey, String secretKey) throws InvalidPortException, InvalidEndpointException {

        minioClient = createMinioClient(endpoint, accessKey, secretKey);

    }

    public MinioClient createMinioClient(String endpoint, String accessKey, String secretKey) throws InvalidPortException, InvalidEndpointException {
        return new MinioClient(endpoint, accessKey, secretKey);
    }

    public void createBucket(String bucketName) throws IOException, InvalidKeyException, NoSuchAlgorithmException, InsufficientDataException, InternalException, NoResponseException, InvalidBucketNameException, XmlPullParserException, ErrorResponseException, RegionConflictException {
        if (!checkIfBucketExists(bucketName))
            minioClient.makeBucket(bucketName);
    }

    public boolean checkIfBucketExists(String bucketName) throws IOException, InvalidKeyException, NoSuchAlgorithmException, InsufficientDataException, InternalException, NoResponseException, InvalidBucketNameException, XmlPullParserException, ErrorResponseException {
        return minioClient.bucketExists(bucketName);
    }

    public LinkedList<String> listObjects(String bucketName) throws XmlPullParserException, InsufficientDataException, NoSuchAlgorithmException, IOException, NoResponseException, InvalidKeyException, InternalException, InvalidBucketNameException, ErrorResponseException {
        LinkedList<String> objectList = new LinkedList<>();

        for (Result<Item> result : minioClient.listObjects(bucketName))
            objectList.add(result.get().objectName());

        return objectList;
    }
    public void putObject(String bucketName, String objectName, String fileName) throws XmlPullParserException, NoSuchAlgorithmException, InvalidKeyException, IOException {
        try{
            // Upload the zip file to the bucket with putObject
            minioClient.putObject(bucketName, objectName, fileName);
            //System.out.println("File is successfully uploaded as " + fileName + " to " + bucketName + " bucket.");
        } catch (MinioException e) {
            //System.out.println("Error occurred: " + e);
        }

    }

}
