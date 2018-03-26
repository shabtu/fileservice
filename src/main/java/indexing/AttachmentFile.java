package indexing;

public class AttachmentFile {

    int sno, bo_sno;
    String name, uniqueId, creationDate, bucket;

    public AttachmentFile(int sno, int bo_sno, String uniqueId, String creationDate, String name, String bucket){
        this.sno = sno;
        this.bo_sno = bo_sno;
        this.name = name;
        this.uniqueId = uniqueId;
        this.creationDate = creationDate;
        this.bucket = bucket;
    }

    @Override
    public String toString(){
        return String.format(
                "File[sno=%d, bo_sno=%d, name='%s', date='%s']",
                sno, bo_sno, creationDate, name);
    }
}
