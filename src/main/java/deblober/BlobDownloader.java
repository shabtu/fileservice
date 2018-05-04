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
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;

@SpringBootApplication
public class BlobDownloader implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(BlobDownloader.class);


    public static LinkedList<AttachmentFile> files = new LinkedList<>();


    public static void main(String args[]) throws IOException, InvalidKeyException, NoSuchAlgorithmException, RegionConflictException, XmlPullParserException, InvalidPortException, InternalException, NoResponseException, InvalidBucketNameException, InsufficientDataException, InvalidEndpointException, ErrorResponseException, SQLException {


        SpringApplication.run(BlobDownloader.class, args);
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Override
    public void run(String... strings) throws Exception {


        String sql = "SELECT SNO, BO_SNO, NAME, UNIQUE_ID, CREATIONDATE, FILEDATA FROM APL_FILE FETCH FIRST 5000 ROWS ONLY";

        log.info("Querying for attachment files");
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



        for (AttachmentFile attachmentFile : files) {

            InputStream inputStream = attachmentFile.getFileData();

            //fileQueue.add(attachmentFile);
            /*File file = new File("download/" + attachmentFile.generateFileNameWithDirectories());
            file.getParentFile().mkdirs();*/

            OutputStream out = new FileOutputStream(new File("downloads/" + attachmentFile.generateFileName()));

            byte[] buff = new byte[4096];
            int len;

            while ((len = inputStream.read(buff)) != -1) {
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