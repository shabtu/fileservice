package common;

import io.minio.errors.*;

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.security.InvalidKeyException;
import org.xmlpull.v1.XmlPullParserException;
import io.minio.MinioClient;


public class FileStorage {

    private final MinioClient minioClient;

    public FileStorage(String endpoint, String accessKey, String secretKey) throws InvalidPortException, InvalidEndpointException, IOException, XmlPullParserException, NoSuchAlgorithmException, RegionConflictException, InvalidKeyException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InternalException {

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
    public void putObject(String bucketName, String objectName, String fileName) throws XmlPullParserException, NoSuchAlgorithmException, InvalidKeyException, IOException {
        try{
            // Upload the zip file to the bucket with putObject
            minioClient.putObject(bucketName, objectName, fileName);
            System.out.println("newFile is successfully uploaded as " + fileName + " to " + bucketName + " bucket.");
        } catch (MinioException e) {
            System.out.println("Error occurred: " + e);
        }

    }

    public void removeBucket(String bucketName) throws IOException, XmlPullParserException, NoSuchAlgorithmException, InvalidKeyException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InternalException {
        if(checkIfBucketExists(bucketName))
            minioClient.removeBucket(bucketName);
    }

    public void getObject(String bucketName, String objectName) throws IOException, InvalidKeyException, NoSuchAlgorithmException, InsufficientDataException, InvalidArgumentException, InternalException, NoResponseException, InvalidBucketNameException, XmlPullParserException, ErrorResponseException {
        InputStream stream = minioClient.getObject(bucketName, objectName);

        FileWriter fileWriter = new FileWriter(new File("downloads/" + objectName));

        byte[] buf = new byte[16384];
        int bytesRead;
        while ((bytesRead = stream.read(buf, 0, buf.length)) >= 0) {
            fileWriter.write(new String(buf, 0, bytesRead));
        }


    }

    public void removeObject(String bucketName, String objectName) {


    }
}
