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

public class FileDownloader extends FileStorage {


    private static final Logger log = LoggerFactory.getLogger(FileDownloader.class);

    private LinkedList<FileInfo> buffer = new LinkedList<>();

    public FileDownloader(String endpoint, String accessKey, String secretKey) throws InvalidPortException, InvalidEndpointException {
        super(endpoint, accessKey, secretKey);
    }

    public synchronized void getObject(String bucketName, String objectName, String folderName) {

        String parsedObjectName = parseFile(objectName).generateFileNameWithDirectories();
        File file = new File(folderName + "/" + parsedObjectName);
        file.getParentFile().mkdirs();
        try {
            log.info("Thread " + currentThread().getId() + " is getting file: " + parsedObjectName );

           //while (!file.exists()) {
                minioClient.getObject(bucketName, parsedObjectName, folderName + "/" + parsedObjectName);
                Thread.sleep(5000);
            //}

            //System.out.println("Name: " + minioClient.statObject(bucketName, parsedObjectName).name());

            /*InputStream inputStream = minioClient.getObject(bucketName, parsedObjectName);

            OutputStream out = new FileOutputStream(file);

            byte[] buff = new byte[4096];
            int len;

            while ((len = inputStream.read(buff)) != -1) {
                out.write(buff, 0, len);
            }
            out.close();*/
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

            getObject(bucketName, fileInfo.generateFileNameWithDirectories(), "retrievedFiles");

        }

        log.info("Thread " + currentThread().getId() + " died");
    }

    public void addToBuffer(FileInfo fileInfo) {
        buffer.add(fileInfo);
    }
}
