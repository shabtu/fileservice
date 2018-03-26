package indexing;

import java.sql.Blob;

public class AttachmentFile {

    Blob fileData;
    int sno, bo_sno;
    String name, uniqueId, creationDate, bucket;

    public AttachmentFile(int sno, int bo_sno, String name, String uniqueId, String creationDate, Blob fileData, String bucket){
        this.sno = sno;
        this.bo_sno = bo_sno;
        this.name = name;
        this.uniqueId = uniqueId;
        this.creationDate = creationDate;
        this.bucket = bucket;
        this.fileData = fileData;
    }

    @Override
    public String toString(){
        return String.format(
                "File[sno=%d, bo_sno=%d, name='%s', date='%s']",
                sno, bo_sno, creationDate, name);
    }

    public String generateFileName(){
        return sno +  "_" + bo_sno + "_" + creationDate + "_" + uniqueId + "_" + name;
    }
}
