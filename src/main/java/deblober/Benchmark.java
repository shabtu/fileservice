package deblober;

import common.FileInfo;
import common.FileStorage;
import indexing.Indexer;
import io.minio.errors.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.support.SqlLobValue;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobHandler;
import org.xmlpull.v1.XmlPullParserException;
import search.FileSearcher;
import search.Search;

import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;

import static indexing.Indexer.BUCKET_NAME;

@SpringBootApplication
public class Benchmark implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(Benchmark.class);


    /* The files that are to be indexed*/
    public List<AttachmentFile> files = new LinkedList<>();

    /* Number of files to index from the database */
    private int numberOfFiles = 4096;

    /*Number of test runs per test size*/
    int numberOfRuns = 10;

    /*Prints results for several runs*/
    StringBuilder sb  = new StringBuilder();

    public static void main(String args[]) {

        SpringApplication.run(Benchmark.class, args);

    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Override
    public void run(String... strings) throws Exception {

        int numberOfIndexThreads = 16;
        Indexer indexer = new Indexer(numberOfIndexThreads);

        files = getFilesFromDatabase(numberOfFiles);

        for (int testSize = 2048; testSize <= numberOfFiles; testSize*=2) {

            log.info("Test size: " + testSize);

            for (int runs = 0; runs < numberOfRuns; runs++) {
                sb.append(indexer.index(files.subList(0, testSize-1))).append(" ");
            }
            log.info("Indexing time (s): " + sb.toString() + "\n");
            sb.delete(0, sb.length());
        }

        indexer = null;


        int numberOfSearchThreads = 32;

        Search search = new Search(numberOfSearchThreads);

        for (int testSize = 32; testSize <= numberOfFiles; testSize*=2) {
            log.info("Test size: " + testSize);
            for (int runs = 0; runs < numberOfRuns; runs++) {
                sb.append(search.runSearch(testSize)).append(" ");
            }
            log.info("Search time (ms): " + sb.toString()  + "\n");
            sb.delete(0, sb.length());
        }


        //dbLatencyTest();

    }

    private void dbLatencyTest() {

        for (int runs = 0; runs < numberOfRuns; runs++) {
            sb.append(dbLatencyCheck()).append(" ");
        }
        log.info("Round-trip time (ms): " + sb.toString() + "\n");
        sb.delete(0, sb.length());

    }

    private long dbLatencyCheck(){

        long startTime = System.nanoTime();
        jdbcTemplate.execute("SELECT 1 FROM DUAL");
        long stopTime = System.nanoTime();

        return (stopTime-startTime)/1000000;
    }

    private void dbInsertionTest(int initialTestSize) throws InvalidPortException, InvalidEndpointException, IOException, InvalidKeyException, NoSuchAlgorithmException, InsufficientDataException, InvalidArgumentException, InternalException, NoResponseException, InvalidBucketNameException, XmlPullParserException, ErrorResponseException {

        FileStorage fileStorage = new FileStorage("http://localhost:9000", "minio", "minio123");
        LinkedList<String> filesToFind = fileStorage.listObjects(BUCKET_NAME);
        LinkedList<FileInfo> files = getFileInfo(filesToFind);
        LinkedList<AttachmentFile> filesWithData = new LinkedList<>();

        for (FileInfo fileInfo : files)
            filesWithData.add(new AttachmentFile(
                    fileInfo.getSno(),
                    fileInfo.getBo_sno(),
                    fileInfo.getCreationDate(),
                    fileInfo.getUniqueId(),
                    fileInfo.getName(),
                    fileStorage.getMinioClient().getObject(BUCKET_NAME, fileInfo.generateFileNameWithDirectories()),
                    BUCKET_NAME));

        for (int testSize = initialTestSize; testSize <= numberOfFiles; testSize*=2) {
            log.info("Test size: " + testSize);
            for (int runs = 0; runs < numberOfRuns; runs++) {
                sb.append(insertFileIntoDatabase(filesWithData, testSize)).append(" ");
            }
            log.info("Insertion time (ms): " + sb.toString()  + "\n");
            sb.delete(0, sb.length());
        }

    }

    private void dbSearchTest(int initialTestSize) throws InvalidPortException, InvalidEndpointException, IOException, InvalidKeyException, NoSuchAlgorithmException, XmlPullParserException, ErrorResponseException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InternalException {

        FileStorage fileStorage = new FileStorage("http://localhost:9000", "minio", "minio123");
        LinkedList<String> filesToFind = fileStorage.listObjects(BUCKET_NAME);
        LinkedList<FileInfo> files = getFileInfo(filesToFind);

        for (int testSize = initialTestSize; testSize <= numberOfFiles; testSize*=2) {
            log.info("Test size: " + testSize);
            for (int runs = 0; runs < numberOfRuns; runs++) {
                sb.append(findFileInDatabase(files, testSize)).append(" ");
            }
            log.info("Search time (ms): " + sb.toString()  + "\n");
            sb.delete(0, sb.length());
        }
    }

    private static FileInfo createAttachmentFile(String file){

        String[] fields = file.split("/");

        return new FileInfo(Integer.parseInt(fields[0]),
                Integer.parseInt(fields[1]),
                fields[2],
                fields[3],
                fields[4],
                BUCKET_NAME);
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

    private LinkedList<FileInfo> getFileInfo(LinkedList<String> fileNames){
        LinkedList<FileInfo> fileInfos = new LinkedList<>();

        for (String fileName : fileNames)
            fileInfos.add(createAttachmentFile(fileName));

        return fileInfos;
    }

    private long insertFileIntoDatabase(LinkedList<AttachmentFile> files, int testSize) {

        long startTime = System.nanoTime();

        for (int i = 0; i < testSize-1; i++) {

            String getSql = "INSERT INTO FILESERVICETEST (SNO, BO_SNO, CREATIONDATE, UNIQUE_ID, NAME, FILEDATA) VALUES (?, ?, ?, ?, ?, ?)";

            int fileIndex = i;
            jdbcTemplate.update(connection -> {
                AttachmentFile file = files.get(fileIndex);
                PreparedStatement preparedStatement = connection.prepareStatement(getSql);
                preparedStatement.setInt(1, file.getSno());
                preparedStatement.setInt(2, file.getBo_sno());
                preparedStatement.setDate(3, new Date(System.currentTimeMillis()));
                preparedStatement.setString(4, file.getUniqueId());
                preparedStatement.setString(5, file.getName());
                preparedStatement.setBlob(6, file.getFileData());
                return preparedStatement;
            });

        }

        long stopTime = System.nanoTime();

        return (stopTime-startTime)/1000000;
    }

    private long findFileInDatabase(LinkedList<FileInfo> files, int testSize){

        long startTime = System.nanoTime();

        for (int i = 0; i < testSize-1; i++) {

            String getSql = "SELECT NAME FROM APL_FILE WHERE " +
                    "SNO=" + files.get(i).getSno() +
                    " AND BO_SNO=" + files.get(i).getBo_sno() +
                    //" AND CREATIONDATE='" + files.get(i).getCreationDate() + "'" +
                    " AND UNIQUE_ID='" + files.get(i).getUniqueId() + "'";


            jdbcTemplate.execute(getSql);
        }

        long stopTime = System.nanoTime();

        return (stopTime-startTime)/1000000;
    }

    private List<AttachmentFile> getFilesFromDatabase(int testSize){

        String sql = "SELECT SNO, BO_SNO, NAME, UNIQUE_ID, CREATIONDATE, FILEDATA, FILESIZE FROM APL_FILE FETCH FIRST " + testSize + " ROWS ONLY";

        return jdbcTemplate.query(
                sql,
                (rs, rowNum) -> new AttachmentFile(
                        rs.getInt("SNO"),
                        rs.getInt("BO_SNO"),
                        rs.getDate("CREATIONDATE").toString(),
                        rs.getString("UNIQUE_ID"),
                        nameFormatter(rs.getString("NAME")),
                        rs.getBlob("FILEDATA").getBinaryStream(),
                        "vismaproceedoaplfile")
        );

    }
}