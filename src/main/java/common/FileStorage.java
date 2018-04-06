package common;

import deblober.AttachmentFile;
import deblober.BlobDownloader;
import indexing.Indexer;
import io.minio.errors.*;

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.security.InvalidKeyException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlpull.v1.XmlPullParserException;
import io.minio.MinioClient;

import static deblober.BlobDownloader.fileQueue;


public class FileStorage extends Thread {


    private static final Logger log = LoggerFactory.getLogger(FileStorage.class);
    int id;

    private final MinioClient minioClient;
    private LinkedList<AttachmentFile> buffer = new LinkedList<>();

    public FileStorage(String endpoint, String accessKey, String secretKey, int id) throws InvalidPortException, InvalidEndpointException {

        this.id = id;
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

    public void putObjectStream(String bucketName, String objectName, InputStream inputStream) throws XmlPullParserException, NoSuchAlgorithmException, InvalidKeyException, IOException {
        try{
            // Upload the zip file to the bucket with putObject
            minioClient.putObject(bucketName, objectName, inputStream, "document");
            System.out.println("newFile is successfully uploaded as " + objectName + " to " + bucketName + " bucket.");
        } catch (MinioException e) {
            System.out.println("Error occurred: " + e);
        }

    }

    public void getObject(String bucketName, String objectName, String folderName) throws IOException, InvalidKeyException, NoSuchAlgorithmException, InsufficientDataException, InvalidArgumentException, InternalException, NoResponseException, InvalidBucketNameException, XmlPullParserException, ErrorResponseException {

        minioClient.getObject(bucketName, objectName, folderName + "/" + objectName);

    }

    @Override
    public void run(){

        AttachmentFile attachmentFile;
        while (fileQueue.peek() != null) {
            log.info("Thread " + id + " is testing..");
            attachmentFile = fileQueue.remove();
            try {
                putObjectStream("images", attachmentFile.generateFileName(), attachmentFile.getFileData().getBinaryStream());
            } catch (XmlPullParserException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (InvalidKeyException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }

        log.info("Thread " + id + " died");

    }

    public void addToBuffer(AttachmentFile attachmentFile) {
        buffer.add(attachmentFile);
    }
}
