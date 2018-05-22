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
import java.util.concurrent.atomic.LongAdder;

import static search.Search.BUCKET_NAME;

public class FileSearcher extends FileStorage {


    private static final Logger log = LoggerFactory.getLogger(FileSearcher.class);
    private final Search search;

    private LinkedList<FileInfo> buffer = new LinkedList<>();

    public FileSearcher(String endpoint, String accessKey, String secretKey, Search search) throws InvalidPortException, InvalidEndpointException, IOException, XmlPullParserException, NoSuchAlgorithmException, RegionConflictException, InvalidKeyException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InternalException {
        super(endpoint, accessKey, secretKey);
        this.search = search;
    }

    public void findObject(String bucketName, String objectName) {

        String parsedObjectName = parseFile(objectName).generateFileNameWithDirectories();

        try {

            //System.out.println("Found: " + minioClient.statObject(bucketName, parsedObjectName).name() + "[" + search.searchCounter.get() + "]");
            minioClient.statObject(bucketName, parsedObjectName);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /*Parse the file info and create a FileInfo*/
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

        /*Get each file info in the buffer to find on Minio*/
        while (buffer.peek() != null) {
            fileInfo = buffer.remove();

            /*Remove the citation in front of the bucket name*/
            bucketName  = fileInfo.getBucket().substring(1, fileInfo.getBucket().length());

            /*Find the object on Minio*/
            findObject(bucketName, fileInfo.generateFileNameWithDirectories());

            if (search.searchCounter.get() >= search.numberOfFiles)
                break;
            else {
                //System.out.println("Found " + search.searchCounter.getAndIncrement());
                search.searchCounter.getAndIncrement();
            }

        }

        /*When there are no more files to be found on Minio the thread dies*/
        //log.info("Thread " + currentThread().getId() + " died");
    }

    public void addToBuffer(FileInfo fileInfo) {
        buffer.add(fileInfo);
    }
}
