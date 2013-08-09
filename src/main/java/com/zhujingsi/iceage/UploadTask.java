package com.zhujingsi.iceage;

import java.io.FileNotFoundException;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.UploadArchiveRequest;
import com.amazonaws.services.glacier.model.UploadArchiveResult;
import com.amazonaws.services.s3.internal.RepeatableFileInputStream;
import com.zhujingsi.iceage.Manifest.FileEntry;

public class UploadTask implements Runnable {
    private FileEntry fileToUpload;

    private static Manifest manifest;

    private static AmazonGlacierClient client;

    private static AtomicInteger counter = new AtomicInteger();
    
    public static AtomicInteger toUpload = new AtomicInteger();
    
    private UpdateTransaction updateTransaction;
    
    public UploadTask withUpdateTransaction(UpdateTransaction t) {
        updateTransaction = t;
        return this;
    }

    public static void setManifest(Manifest m) {
        manifest = m;
    }

    public static void setClient(AmazonGlacierClient client) {
        UploadTask.client = client;
    }

    public UploadTask(FileEntry fe) {
        fileToUpload = fe;
    }

    public static int getCount() {
        return counter.get();
    }

    public void run() {
        try {
            int retryCounter = 3;
            long sleepTime = 200;
            while (retryCounter > 0) {
                retryCounter--;
                try {
                    UploadArchiveRequest request = new UploadArchiveRequest().withVaultName(manifest.getVault())
                            .withBody(new RepeatableFileInputStream(fileToUpload.file))
                            .withChecksum(fileToUpload.treeHash).withContentLength(fileToUpload.file.length());

                    UploadArchiveResult result = client.uploadArchive(request);
                    fileToUpload.archiveId = result.getArchiveId();
                    if (updateTransaction != null) {
                        updateTransaction.newArchiveId = result.getArchiveId();
                    }
                    counter.incrementAndGet();
                    break;
                } catch (AmazonServiceException e) {
                    if (retryCounter > 0) {
                        System.out.println("Lost contact with Amazon Service. Retrying...");
                    } else {
                        System.out.println("Lost contact with Amazon Service. Giving up.");
                    }
                }
                Thread.sleep(sleepTime);
                sleepTime *= 2;
            }
        } catch (FileNotFoundException e) {
            System.out.println("Failed to find the file!");
        } catch (AmazonClientException e) {
            System.out.println("Client Failed" + e.getMessage());
            e.printStackTrace();
        } catch (InterruptedException e) {

        }
    }

}
