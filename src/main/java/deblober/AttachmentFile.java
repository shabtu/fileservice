package deblober;

import common.FileInfo;
import jdk.internal.util.xml.impl.Input;

import java.io.InputStream;
import java.sql.Blob;

public class AttachmentFile extends FileInfo {

    InputStream fileData;

    public AttachmentFile(int sno, int bo_sno, String creationDate, String uniqueId, String name, InputStream fileData, String bucket){
        super(sno, bo_sno, creationDate, uniqueId, name, bucket);
        this.fileData = fileData;
    }
    public InputStream getFileData() {
        return fileData;
    }


    public String generateFileName(){
        return getSno() +  "_" + getBo_sno() + "_" + getCreationDate() + "_" + getUniqueId() + "_" + getName();
    }
}
