package deblober;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.util.LinkedList;

@SpringBootApplication
public class Application implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String args[]) {

        SpringApplication.run(Application.class, args);
    }

    @Autowired
    JdbcTemplate jdbcTemplate;
    LinkedList<AttachmentFile> files = new LinkedList<>();

    @Override
    public void run(String... strings) throws Exception {

        log.info("Creating tables");

        String sql = "SELECT SNO, BO_SNO, NAME, UNIQUE_ID, CREATIONDATE, FILEDATA FROM APL_FILE FETCH FIRST 10 ROWS ONLY";

        log.info("Querying for customer records where first_name = 'Josh':");
        jdbcTemplate.query(
                sql,
                (rs, rowNum) -> new AttachmentFile(
                        rs.getInt("SNO"),
                        rs.getInt("BO_SNO"),
                        rs.getString("NAME"),
                        rs.getString("UNIQUE_ID"),
                        rs.getDate("CREATIONDATE").toString(),
                        rs.getBlob("FILEDATA"),
                        "images")
        ).forEach(attachmentFile -> files.add(attachmentFile));


        for (AttachmentFile file : files) {
            InputStream stream = file.getFileData().getBinaryStream();

            FileWriter fileWriter = new FileWriter(new File("downloads/" + file.generateFileName()));

            byte[] buf = new byte[16384];
            int bytesRead;
            while ((bytesRead = stream.read(buf, 0, buf.length)) >= 0) {
                fileWriter.write(new String(buf, 0, bytesRead));
            }
        }

    }
}