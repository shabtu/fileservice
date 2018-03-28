package deblober;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.*;
import java.util.LinkedList;

@SpringBootApplication
public class BlobDownloader implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(BlobDownloader.class);

    public static void main(String args[]) {

        SpringApplication.run(BlobDownloader.class, args);
    }

    @Autowired
    JdbcTemplate jdbcTemplate;
    LinkedList<AttachmentFile> files = new LinkedList<>();

    @Override
    public void run(String... strings) throws Exception {

        String sql = "SELECT SNO, BO_SNO, NAME, UNIQUE_ID, CREATIONDATE, FILEDATA FROM APL_FILE FETCH FIRST 200 ROWS ONLY";

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

        for (AttachmentFile attachmentFile : files) {
            InputStream stream = attachmentFile.getFileData().getBinaryStream();

            log.info("Writing file: " + attachmentFile.generateFileName());
            OutputStream out = new FileOutputStream(new File("downloads/" + attachmentFile.generateFileName()));
            byte[] buff = new byte[4096];  // how much of the blob to read/write at a time
            int len;

            while ((len = stream.read(buff)) != -1) {
                out.write(buff, 0, len);
            }

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
}