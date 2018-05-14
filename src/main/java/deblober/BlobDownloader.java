package deblober;

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
import org.xmlpull.v1.XmlPullParserException;
import search.Search;

import java.io.*;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Date;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;

@SpringBootApplication
public class BlobDownloader implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(BlobDownloader.class);


    /* The files that are to be indexed*/
    public static LinkedList<AttachmentFile> files = new LinkedList<>();

    /* Number of files to index from the database */
    private int numberOfFiles = 5000;


    public static void main(String args[]) {

        SpringApplication.run(BlobDownloader.class, args);

    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Override
    public void run(String... strings) throws Exception {



        log.info("Querying for attachment files");


        /*Returns the amount of files specified from the database */
        getFilesFromDatabase();

        log.info("Got " + files.size() + " attachment files");


        Indexer indexer = new Indexer(numberOfFiles);
        Search search = new Search(numberOfFiles);

        long indexStartTime = System.nanoTime();
        indexer.index(files);
        long indexStopTime = System.nanoTime();

        long indexingTime = (indexStopTime-indexStartTime)/1000000000;

        log.info("Indexing time: " + indexingTime/60 + " minute(s) and " + indexingTime%60 + " second(s).");

        long searchStartTime = System.nanoTime();
        search.runSearch();
        long searchStopTime = System.nanoTime();

        long searchTime = (searchStopTime-searchStartTime)/1000000000;

        log.info("Search time: " + searchTime/60 + " minute(s) and " + searchTime%60 + " second(s).");


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

    private void getFilesFromDatabase(){

        String sql = "SELECT SNO, BO_SNO, NAME, UNIQUE_ID, CREATIONDATE, FILEDATA, FILESIZE FROM APL_FILE FETCH FIRST " + numberOfFiles + " ROWS ONLY";

        jdbcTemplate.query(
                sql,
                (rs, rowNum) -> new AttachmentFile(
                        rs.getInt("SNO"),
                        rs.getInt("BO_SNO"),
                        rs.getDate("CREATIONDATE").toString(),
                        rs.getString("UNIQUE_ID"),
                        nameFormatter(rs.getString("NAME")),
                        rs.getBlob("FILEDATA").getBinaryStream(),
                        "vismaproceedoaplfile")
        ).forEach(attachmentFile -> files.add(attachmentFile));
    }
}