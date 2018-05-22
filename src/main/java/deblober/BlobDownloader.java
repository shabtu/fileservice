package deblober;

import indexing.Indexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;
import search.Search;

import java.util.LinkedList;
import java.util.List;

@SpringBootApplication
public class BlobDownloader implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(BlobDownloader.class);


    /* The files that are to be indexed*/
    public List<AttachmentFile> files = new LinkedList<>();

    /* Number of files to index from the database */
    private int numberOfFiles = 2048;


    public static void main(String args[]) {

        SpringApplication.run(BlobDownloader.class, args);

    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Override
    public void run(String... strings) throws Exception {

        /*Returns the amount of files specified from the database */

        int numberOfIndexThreads = 1;
        Indexer indexer = new Indexer(numberOfIndexThreads);
        files = getFilesFromDatabase(numberOfFiles);
        for (int testSize = 32; testSize < numberOfFiles; testSize*=2) {
            log.info("Test size: " + testSize);

            indexer.index(files.subList(0, testSize-1));
        }

        int numberOfSearchThreads = 1;

        Search search = new Search(numberOfSearchThreads);

        for (int testSize = 32; testSize <= numberOfFiles; testSize*=2) {
            log.info("Test size: " + testSize);
            search.runSearch(testSize);
        }
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