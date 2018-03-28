package deblober;

import common.FileInfo;

import java.sql.Blob;

public class AttachmentFile extends FileInfo {

    Blob fileData;

    public AttachmentFile(int sno, int bo_sno, String creationDate, String uniqueId, String name, Blob fileData, String bucket){
        super(sno, bo_sno, creationDate, uniqueId, name, bucket);
        this.fileData = fileData;
    }
    public Blob getFileData() {
        return fileData;
    }

    public String generateFileName(){
        return getSno() +  "_" + getBo_sno() + "_" + getCreationDate() + "_" + getUniqueId() + "_" + getName();
    }
}
