package search;

import common.FileInfo;
import common.FileStorage;
import deblober.AttachmentFile;
import io.minio.errors.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlpull.v1.XmlPullParserException;

import java.io.File;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;

public class FileDownloader extends FileStorage {


    private static final Logger log = LoggerFactory.getLogger(FileDownloader.class);

    private LinkedList<FileInfo> buffer = new LinkedList<>();

    public FileDownloader(String endpoint, String accessKey, String secretKey) throws InvalidPortException, InvalidEndpointException {
        super(endpoint, accessKey, secretKey);
    }

    public synchronized void getObject(String bucketName, String objectName, String folderName) throws IOException, InvalidKeyException, NoSuchAlgorithmException, InsufficientDataException, InvalidArgumentException, InternalException, NoResponseException, InvalidBucketNameException, XmlPullParserException, ErrorResponseException {

        objectName = parseFile(objectName).generateFileNameWithDirectories();
        File file = new File(folderName + "/" + objectName);
            file.getParentFile().mkdirs();
        minioClient.getObject("images", objectName, folderName + "/" + objectName);

    }

    private FileInfo parseFile(String fileName){

        String[] fields = fileName.split("/");

        return new FileInfo(Integer.parseInt(fields[0]),
                Integer.parseInt(fields[1]),
                fields[2],
                fields[3],
                fields[4].substring(0,fields[4].length()-1),
                "images");
    }

    @Override
    public void run(){
        FileInfo fileInfo;
        while (buffer.peek() != null) {
             fileInfo = buffer.remove();
            log.info("Thread " + currentThread().getId() + " is getting file: " + fileInfo.getName());
            try {
                getObject(fileInfo.getBucket(), fileInfo.generateFileNameWithDirectories(), "retrievedFiles");
            } catch (XmlPullParserException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (InvalidKeyException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InsufficientDataException e) {
                e.printStackTrace();
            } catch (InvalidArgumentException e) {
                e.printStackTrace();
            } catch (ErrorResponseException e) {
                e.printStackTrace();
            } catch (NoResponseException e) {
                e.printStackTrace();
            } catch (InvalidBucketNameException e) {
                e.printStackTrace();
            } catch (InternalException e) {
                e.printStackTrace();
            }

        }

        log.info("Thread " + currentThread().getId() + " died");
    }

    public void addToBuffer(FileInfo fileInfo) {
        buffer.add(fileInfo);
    }
}
