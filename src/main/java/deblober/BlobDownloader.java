package deblober;

import common.FileStorage;
import io.minio.errors.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;
import org.xmlpull.v1.XmlPullParserException;

import java.io.*;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;

@SpringBootApplication
public class BlobDownloader implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(BlobDownloader.class);

    private static final String MINIO_ENDPOINT = "http://localhost:9000";
    private static final String ACCESS_KEY = "minio";
    private static final String SECRET_KEY = "minio123";
    private static final String BUCKET_NAME = "images";
    public static LinkedBlockingQueue<AttachmentFile> fileQueue = new LinkedBlockingQueue<>();

    public static void main(String args[]) {

        SpringApplication.run(BlobDownloader.class, args);
    }

    @Autowired
    JdbcTemplate jdbcTemplate;
    LinkedList<AttachmentFile> files = new LinkedList<>();

    @Override
    public void run(String... strings) throws Exception {


        String sql = "SELECT SNO, BO_SNO, NAME, UNIQUE_ID, CREATIONDATE, FILEDATA FROM APL_FILE FETCH FIRST 1000 ROWS ONLY";

        log.info("Querying for attachment files");
        jdbcTemplate.query(
                sql,
                (rs, rowNum) -> new AttachmentFile(
                        rs.getInt("SNO"),
                        rs.getInt("BO_SNO"),
                        rs.getDate("CREATIONDATE").toString(),
                        rs.getString("UNIQUE_ID"),
                        nameFormatter(rs.getString("NAME")),
                        rs.getBlob("FILEDATA"),
                        "images")
        ).forEach(attachmentFile -> files.add(attachmentFile));

        int numberOfThreads = 10;

        FileStorage[] fileStorages = initiateFileStorages(numberOfThreads);

        int distribution = 0, index;

        for (AttachmentFile attachmentFile : files) {

            log.info("Adding file: " + attachmentFile.generateFileName());

            fileQueue.add(attachmentFile);
            /* OutputStream out = new FileOutputStream(new File("downloads/" + attachmentFile.generateFileName()));
            byte[] buff = new byte[4096];  // how much of the blob to read/write at a time
            int len;

            while ((len = stream.read(buff)) != -1) {
                //out.write(buff, 0, len);
            }*/

        }

        runFileStorageThreads(fileStorages);

    }

    private void runFileStorageThreads(FileStorage[] fileStorages) {
        for (FileStorage fileStorage : fileStorages) {
            fileStorage.run();
        }
    }

    public static LinkedBlockingQueue<AttachmentFile> getFiles() {
        return fileQueue;
    }

    private FileStorage[] initiateFileStorages(int numberOfThreads) throws InvalidPortException, InvalidEndpointException, IOException, XmlPullParserException, NoSuchAlgorithmException, InvalidKeyException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InternalException, RegionConflictException {

        log.info("Initiating storage threads..");

        FileStorage[] fileStorages = new FileStorage[numberOfThreads];

        for (int i = 0; i < fileStorages.length ; i++) {

            fileStorages[i] = new FileStorage(MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY, i);

            if (!fileStorages[i].checkIfBucketExists(BUCKET_NAME))
                fileStorages[i].createBucket(BUCKET_NAME);

        }

        return fileStorages;
    }

    private String nameFormatter(String name) {

        String[] nameParts = name.split("_");
        StringBuilder nameBuilder = new StringBuilder();

        for (int i = 0; i < nameParts.length; i++) {
            if (i == 0)
                nameBuilder.append(nameParts[i]);
            else
                nameBuilder.append(nameParts[i].substring(0,1).toUpperCase() + nameParts[i].substring(1));
        }

        return nameBuilder.toString();
    }
}