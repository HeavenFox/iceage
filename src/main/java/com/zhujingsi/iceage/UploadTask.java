package com.zhujingsi.iceage;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.UploadArchiveRequest;
import com.amazonaws.services.glacier.model.UploadArchiveResult;
import com.amazonaws.services.s3.internal.RepeatableFileInputStream;
import com.zhujingsi.iceage.Manifest.FileEntry;

public class UploadTask implements Runnable
{
    private FileEntry fileToUpload;
    
    private static Manifest manifest;
    
    private static AmazonGlacierClient client;
    
    private static AtomicInteger counter = new AtomicInteger();
    
    public static void setManifest(Manifest m) {
        manifest = m;
    }

    public static void setClient(AmazonGlacierClient client)
    {
        UploadTask.client = client;
    }


    public UploadTask(FileEntry fe)
    {
        fileToUpload = fe;
    }
    
    public static int getCount()
    {
        return counter.get();
    }

    public void run()
    {
        try
        {
            UploadArchiveRequest request = new UploadArchiveRequest().withVaultName(manifest.getVault())
                                                                     .withBody(new RepeatableFileInputStream(fileToUpload.file))
                                                                     .withChecksum(fileToUpload.treeHash)
                                                                     .withContentLength(fileToUpload.file.length());
            
            UploadArchiveResult result = client.uploadArchive(request);
            fileToUpload.archiveId = result.getArchiveId();
            counter.incrementAndGet();
        }
        catch (FileNotFoundException e)
        {
            System.out.println("Failed find the file!");
        }
        catch (AmazonServiceException e)
        {
            System.out.println("Amazon Service failed");
            e.printStackTrace();
        }
        catch (AmazonClientException e)
        {
            System.out.println("Client Failed" + e.getMessage());
            e.printStackTrace();
        }
    }


}
