package com.zhujingsi.iceage;

import com.zhujingsi.iceage.Manifest.FileEntry;

public class RenameTransaction extends Transaction {
    public String oldPath;
    public String newPath;
    public String treeHash;
    public String archiveId;
    
    public void setOldFileEntry(FileEntry f) {
        oldPath = f.path;
        if (f.treeHash != null)
            treeHash = f.treeHash;
        if (f.archiveId != null)
            archiveId = f.archiveId;
    }
    
    public void setNewFileEntry(FileEntry f) {
        newPath = f.path;
        if (f.treeHash != null)
            treeHash = f.treeHash;
        if (f.archiveId != null)
            archiveId = f.archiveId;
    }

}
