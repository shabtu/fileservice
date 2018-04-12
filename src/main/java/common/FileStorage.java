package common;

import deblober.AttachmentFile;
import io.minio.errors.*;

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.security.InvalidKeyException;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlpull.v1.XmlPullParserException;
import io.minio.MinioClient;



public class FileStorage extends Thread {


    private static final Logger log = LoggerFactory.getLogger(FileStorage.class);

    private final MinioClient minioClient;
    private LinkedList<AttachmentFile> buffer = new LinkedList<>();

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
            System.out.println("File is successfully uploaded as " + fileName + " to " + bucketName + " bucket.");
        } catch (MinioException e) {
            System.out.println("Error occurred: " + e);
        }

    }

    public void putObjectStream(String bucketName, String objectName, InputStream inputStream) throws XmlPullParserException, NoSuchAlgorithmException, InvalidKeyException, IOException {
        try{
            // Upload the zip file to the bucket with putObject
            minioClient.putObject(bucketName, objectName, inputStream, "document");
            System.out.println("File is successfully uploaded as " + objectName + " to " + bucketName + " bucket.");
        } catch (MinioException e) {
            System.out.println("Error occurred: " + e);
        }

    }

    public void getObject(String bucketName, String objectName, String folderName) throws IOException, InvalidKeyException, NoSuchAlgorithmException, InsufficientDataException, InvalidArgumentException, InternalException, NoResponseException, InvalidBucketNameException, XmlPullParserException, ErrorResponseException {

        minioClient.getObject(bucketName, objectName, folderName + "/" + parseFile(objectName).generateFileName());

    }

    private FileInfo parseFile(String fileName){



        String[] fields = fileName.split("/");

        return new FileInfo(Integer.parseInt(fields[0]),
                Integer.parseInt(fields[1]),
                fields[2],
                fields[3],
                fields[4],
                "images");
    }

    @Override
    public void run(){

        AttachmentFile attachmentFile;
        while (buffer.peek() != null) {
            attachmentFile = buffer.remove();
            log.info("Thread " + currentThread().getId() + "is adding file: " + attachmentFile.getName());
            try {
                putObjectStream("images", attachmentFile.generateFileNameWithDirectories(), attachmentFile.getFileData());
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

        log.info("Thread " + currentThread().getId() + " died");

    }

    public void addToBuffer(AttachmentFile attachmentFile) {
        buffer.add(attachmentFile);
    }
}
