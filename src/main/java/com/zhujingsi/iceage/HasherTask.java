package com.zhujingsi.iceage;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.services.glacier.TreeHashGenerator;
import com.zhujingsi.iceage.Manifest.FileEntry;

public class HasherTask implements Runnable
{
    private FileEntry fileToHash;
    private static ExecutorService uploaders;
    private static Manifest manifest;
    
    private static AtomicInteger counter = new AtomicInteger();
    
    public static void setUploaders(ExecutorService serv) {
        uploaders = serv;
    }
    
    public static void setManifest(Manifest m) {
        manifest = m;
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
        fileToHash.treeHash = hash;
        
        counter.incrementAndGet();
        if (manifest.addEntry(hash, fileToHash))
        {
            uploaders.execute(new UploadTask(this.fileToHash));
        }
    }
}
