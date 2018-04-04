package common;

import indexing.Indexer;
import io.minio.errors.*;

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.security.InvalidKeyException;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;

import org.xmlpull.v1.XmlPullParserException;
import io.minio.MinioClient;


public class FileStorage extends Thread {

    private final MinioClient minioClient;
    private LinkedList<File> buffer = new LinkedList<>();

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
    public void putObject(String bucketName, String objectName, String fileName) throws XmlPullParserException, NoSuchAlgorithmException, InvalidKeyException, IOException {
        try{
            // Upload the zip file to the bucket with putObject
            minioClient.putObject(bucketName, objectName, fileName);
            System.out.println("newFile is successfully uploaded as " + fileName + " to " + bucketName + " bucket.");
        } catch (MinioException e) {
            System.out.println("Error occurred: " + e);
        }

    }

    public void getObject(String bucketName, String objectName, String folderName) throws IOException, InvalidKeyException, NoSuchAlgorithmException, InsufficientDataException, InvalidArgumentException, InternalException, NoResponseException, InvalidBucketNameException, XmlPullParserException, ErrorResponseException {

        minioClient.getObject(bucketName, objectName, folderName + "/" + objectName);

    }

    @Override
    public void run(){

        File file;
        while (buffer.peek() != null) {
            file = buffer.remove();
            try {
                putObject("images", file.getName(), file.getAbsolutePath());
            } catch (XmlPullParserException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (InvalidKeyException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }

    public void addToBuffer(File file) {
        buffer.add(file);
    }
}
