package com.zhujingsi.iceage;

import com.zhujingsi.iceage.Manifest.FileEntry;

public class UpdateTransaction extends Transaction {
    public String path;
    public String oldArchiveId;
    public String newArchiveId;
    public String oldTreeHash;
    public String newTreeHash;
    
    public void setOldFileEntry(FileEntry f) {
        oldArchiveId = f.archiveId;
        oldTreeHash = f.treeHash;
        path = f.path;
    }
    
    public void setNewFileEntry(FileEntry f) {
        newArchiveId = f.archiveId;
        path = f.path;
    }
}
