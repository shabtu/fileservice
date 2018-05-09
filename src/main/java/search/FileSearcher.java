package search;

import common.FileInfo;
import common.FileStorage;
import deblober.AttachmentFile;
import io.minio.errors.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlpull.v1.XmlPullParserException;

import java.io.*;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;

import static search.Search.BUCKET_NAME;

public class FileSearcher extends FileStorage {


    private static final Logger log = LoggerFactory.getLogger(FileSearcher.class);

    private LinkedList<FileInfo> buffer = new LinkedList<>();

    public FileSearcher(String endpoint, String accessKey, String secretKey) throws InvalidPortException, InvalidEndpointException, IOException, XmlPullParserException, NoSuchAlgorithmException, RegionConflictException, InvalidKeyException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InternalException {
        super(endpoint, accessKey, secretKey);
    }

    public void findObject(String bucketName, String objectName) {

        String parsedObjectName = parseFile(objectName).generateFileNameWithDirectories();
        String fileName = parseFile(objectName).getName();

        try {
            log.info("Thread " + currentThread().getId() + " is getting file: " + fileName );

           System.out.println("Name: " + minioClient.statObject(bucketName, parsedObjectName).name());

            minioClient.statObject(bucketName, parsedObjectName);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private FileInfo parseFile(String fileName){

        String[] fields = fileName.split("/");

        return new FileInfo(Integer.parseInt(fields[0]),
                Integer.parseInt(fields[1]),
                fields[2],
                fields[3],
                fields[4].substring(0,fields[4].length()-1),
                BUCKET_NAME);
    }

    @Override
    public void run(){
        FileInfo fileInfo;
        String bucketName;
        while (buffer.peek() != null) {
            fileInfo = buffer.remove();

            //Remove the " in front of the bucket name
            bucketName  = fileInfo.getBucket().substring(1, fileInfo.getBucket().length());

            findObject(bucketName, fileInfo.generateFileNameWithDirectories());

        }

        log.info("Thread " + currentThread().getId() + " died");
    }

    public void addToBuffer(FileInfo fileInfo) {
        buffer.add(fileInfo);
    }
}
