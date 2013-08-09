package com.zhujingsi.iceage;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.services.glacier.TreeHashGenerator;
import com.zhujingsi.iceage.Manifest.FileEntry;

public class HasherTask implements Runnable
{
    private FileEntry fileToHash;
    private static ExecutorService uploaders;
    private static Manifest manifest;
    private static TransactionLog transactions;
    
    private static AtomicInteger counter = new AtomicInteger();
    public static AtomicInteger toHash = new AtomicInteger();
    
    private UpdateTransaction updateTransaction;
    
    public HasherTask withUpdateTransaction(UpdateTransaction t) {
        updateTransaction = t;
        return this;
    }
    
    public static void setUploaders(ExecutorService serv) {
        uploaders = serv;
    }
    
    public static void setManifest(Manifest m) {
        manifest = m;
    }

    public static void setTransactions(TransactionLog transactions) {
        HasherTask.transactions = transactions;
    }

    public HasherTask(FileEntry f)
    {
        fileToHash = f;
    }

    public static int getCount()
    {
        return counter.get();
    }

    public void run()
    {
        String hash = TreeHashGenerator.calculateTreeHash(this.fileToHash.file);
        
        if (updateTransaction != null) {
            updateTransaction.newTreeHash = hash;
        }
        
        if (fileToHash.treeHash != null && !hash.equals(fileToHash.treeHash)) {
            // Corrupted!
            manifest.addCorrupted(fileToHash);
        } else {
            fileToHash.treeHash = hash;
            
            FileEntry oldEntry = manifest.entryForHash(hash);
            
            if (oldEntry != null) {
                if (oldEntry.isBrandNew()) {
                    // Duplicate
                    EliminateDuplicateTransaction tr = new EliminateDuplicateTransaction();
                    tr.canonicalPath = oldEntry.path;
                    tr.duplicatePath = fileToHash.path;
                    transactions.append(tr);
                } else {
                    // Rename
                    RenameTransaction tr = new RenameTransaction();
                    tr.setNewFileEntry(fileToHash);
                    tr.setOldFileEntry(oldEntry);
                    transactions.append(tr);
                    oldEntry.update(fileToHash);
                }
            } else {
                counter.incrementAndGet();
                if (manifest.putEntry(hash, fileToHash))
                {
                    UploadTask.toUpload.incrementAndGet();
                    uploaders.execute(new UploadTask(this.fileToHash).withUpdateTransaction(updateTransaction));
                }
            }
        }
    }
}
