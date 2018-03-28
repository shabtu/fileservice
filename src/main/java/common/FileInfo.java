package common;


public class FileInfo {

    private int sno, bo_sno;
    private String name, uniqueId, creationDate, bucket;

    public FileInfo(int sno, int bo_sno, String creationDate, String uniqueId, String name, String bucket){
        this.sno = sno;
        this.bo_sno = bo_sno;
        this.creationDate = creationDate;
        this.uniqueId = uniqueId;
        this.name = name;
        this.bucket = bucket;
    }

    public int getSno() {
        return sno;
    }

    public int getBo_sno() {
        return bo_sno;
    }

    public String getName() {
        return name;
    }

    public String getUniqueId() {
        return uniqueId;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public String getBucket() {
        return bucket;
    }

    public String generateFileName(){
        return getSno() +  "_" + getBo_sno() + "_" + getCreationDate() + "_" + getUniqueId() + "_" + getName();
    }

    @Override
    public String toString(){
        return String.format(
                "File[sno=%d, bo_sno=%d, name='%s', date='%s']",
                sno, bo_sno, creationDate, name);
    }
}
