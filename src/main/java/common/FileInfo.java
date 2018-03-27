package indexing;


public class FileInfo {

    int sno, bo_sno;
    String name, uniqueId, creationDate, bucket;

    public FileInfo(int sno, int bo_sno, String name, String uniqueId, String creationDate, String bucket){
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
