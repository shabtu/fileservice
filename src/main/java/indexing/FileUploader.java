package indexing;

import common.FileStorage;
import deblober.AttachmentFile;
import io.minio.errors.InvalidEndpointException;
import io.minio.errors.InvalidPortException;
import io.minio.errors.MinioException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlpull.v1.XmlPullParserException;

import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;

import static indexing.Indexer.BUCKET_NAME;

public class FileUploader extends FileStorage {

    /*Buffer used for storing files that are to be uploaded*/
    private LinkedList<AttachmentFile> buffer = new LinkedList<>();

    private static final Logger log = LoggerFactory.getLogger(FileUploader.class);


    public FileUploader(String endpoint, String accessKey, String secretKey) throws InvalidPortException, InvalidEndpointException {
        super(endpoint, accessKey, secretKey);
    }

    public void putObjectStream(String bucketName, String objectName, InputStream inputStream) throws XmlPullParserException, NoSuchAlgorithmException, InvalidKeyException, IOException {
        try{
            /* Upload the file to the bucket with putObject*/
            minioClient.putObject(bucketName, objectName, inputStream, "document");
            System.out.println("File is successfully uploaded as " + objectName + " to " + bucketName + " bucket.");
        } catch (MinioException e) {
            System.out.println("Error occurred: " + e);
        }

    }

    @Override
    public void run(){

        AttachmentFile attachmentFile;

        /*All files in the buffer are uploaded to the Minio storage*/
        while (buffer.peek() != null) {
            attachmentFile = buffer.remove();
            try {
                putObjectStream(BUCKET_NAME,  attachmentFile.generateFileNameWithDirectories(), attachmentFile.getFileData());
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

        /*When there is nothing else in the buffer the thread dies*/
        log.info("Thread " + currentThread().getId() + " died");

    }

    /*Adds the files to the buffer for upload*/
    public void addToBuffer(AttachmentFile attachmentFile) {
        buffer.add(attachmentFile);
    }
}
