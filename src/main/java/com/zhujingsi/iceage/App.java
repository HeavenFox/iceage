package com.zhujingsi.iceage;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.zhujingsi.iceage.Manifest.FileEntry;

/**
 * Hello world!
 * 
 */
public class App {
    private static Manifest manifest;
    private static TransactionLog transactions;
    private static int filesToHash = 0;
    
    private static void listFiles(File cur, List<Pattern> ignorePatterns, List<File> files) {
        for (File fn : cur.listFiles()) {
            boolean ignore = false;
            for (Pattern pattern : ignorePatterns) {
                if (pattern.matcher(fn.getPath()).find()) {
                    ignore = true;
                    break;
                }
            }
            if (ignore) {
                continue;
            }
            if (fn.isDirectory()) {
                listFiles(fn, ignorePatterns, files);
            } else {
                files.add(fn);
            }
        }
    }
    
    private static void printStatus() {
        System.out.print("Hashed: " + HasherTask.getCount() + "/" + filesToHash);
        System.out.print(" - ");
        System.out.print("Uploaded: " + UploadTask.getCount() + "/" + UploadTask.toUpload.get());
        System.out.print("          \r");
    }
    
    private static void flush() {
        try {
            manifest.flush();
            transactions.flush();
        } catch (IOException e) {
            System.out.println("Cannot flush");
        }
    }
    
    private static void flush(AmazonS3Client client) {
        byte[] bytes = transactions.toJson().getBytes();
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(bytes.length);
        metadata.setContentType("application/json");
        client.putObject("bkt", "transactions.json", new ByteArrayInputStream(bytes), metadata);
    }
    
    private static Options buildOptions() {
        Options options = new Options();
        options.addOption("n", false, "Dry run (do not upload to Amazon)");
        options.addOption("m", false, "Files are considered immutable (do not compare timestamp)");
        options.addOption("p", false, "Paranoid mode (hash every file). Can detect corruption.");
        options.addOption("d", true, "Directory");
        options.addOption("h", true, "Num of hashing threads");
        options.addOption("u", true, "Num of uploading threads");
        
        return options;
    }

    public static void main(String[] args) throws InterruptedException {
        // Command line options
        Options options = buildOptions();

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e2) {
            System.out.println("Unable to parse options");
        }
        
        ExecutorService hashers = Executors.newFixedThreadPool(5);
        ExecutorService uploaders = Executors.newFixedThreadPool(5);
        
        // Manifest
        try {
            manifest = Manifest.readManifest("manifest.json");
            System.out.println("Manifest loaded.");
        } catch (Exception e) {
            System.out.println("Malformed or missing manifest. Creating one.");
            manifest = new Manifest(new File("manifest.json"));
        }
        
        // Transaction Log
        File transactionFile = new File("transactions.json");
        try {
            transactions = TransactionLog.readTransaction(transactionFile);
            System.out.println("Transaction loaded.");
        } catch (Exception e) {
            
        }
        if (transactions == null) {
            transactions = new TransactionLog();
        }
        transactions.transactionFile = transactionFile;
        
        
        
        // Amazon stuff
        AWSCredentials credentials = null;
        try {
            credentials = new PropertiesCredentials(new FileInputStream("AwsCredentials.properties"));
        } catch (FileNotFoundException e1) {
            System.out.println("Cannot find AwsCredentials.properties");
            System.exit(1);
        } catch (IOException e1) {
            System.out.println("Cannot read AwsCredentials.properties");
            System.exit(1);
        }

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            if (manifest.getEndpoint() == null || manifest.getEndpoint().length() == 0) {
                System.out.println("Please enter the endpoint. We'll use us-west-2 by default.");
                String endpoint = reader.readLine();
                if (endpoint.length() == 0) {
                    endpoint = "https://glacier.us-west-2.amazonaws.com/";
                }
                manifest.setEndpoint(endpoint);
            }

            if (manifest.getVault() == null || manifest.getVault().length() == 0) {
                System.out.println("Please enter the vault you want to store the files. Make sure it exists:");
                String vaultName = reader.readLine();
                manifest.setVault(vaultName);
            }

        } catch (IOException e) {
            System.out.println("Failed to read input.");
            System.exit(1);
        }

        AmazonGlacierClient client = new AmazonGlacierClient(credentials);
        client.setEndpoint(manifest.getEndpoint());
        
        AmazonS3Client s3client = new AmazonS3Client(credentials);

        // Build files
        List<File> files = new LinkedList<File>();
        List<Pattern> ignore = new LinkedList<Pattern>();
        BufferedReader ignoreReader;
        try {
            ignoreReader = new BufferedReader(new FileReader("backupignore"));

            String line;
            try {
                while ((line = ignoreReader.readLine()) != null) {
                    String trimmed = line.trim();
                    try {
                        ignore.add(Pattern.compile(trimmed));
                    } catch (PatternSyntaxException e) {
                        System.out.println("Ignoring malformed pattern " + trimmed);
                    }

                }
            } catch (IOException e1) {
                System.out.println("Failed to read ignore file");
            }
        } catch (FileNotFoundException e2) {
            // No ignore patterns
        }

        listFiles(new File("."), ignore, files);

        
        // Prepare hasher
        HasherTask.setManifest(manifest);
        HasherTask.setTransactions(transactions);
        HasherTask.setUploaders(uploaders);
        
        UploadTask.setManifest(manifest);
        UploadTask.setClient(client);
        
        boolean paranoid = cmd.hasOption("p");
        
        // Detect file changes
        for (File cur : files) {
            FileEntry entry = manifest.entryForPath(cur.getPath());
            
            if (entry != null) {
                entry.markExists();
                
                // Has the file been changed?
                if (cur.lastModified() > entry.lastModified) {
                    // Changed. Re-upload. Preserve old one into log
                    FileEntry newEntry = new FileEntry(cur);
                    UpdateTransaction tr = new UpdateTransaction();
                    tr.setOldFileEntry(entry);
                    manifest.removeEntry(entry);
                    
                    filesToHash++;
                    hashers.execute(new HasherTask(newEntry).withUpdateTransaction(tr));
                } else {
                    // Not changed.
                    // If paranoid, check for hash
                    if (paranoid) {
                        filesToHash++;
                        hashers.execute(new HasherTask(entry));
                    } else if (!entry.uploaded()) {
                        // Otherwise, upload if necessary
                        UploadTask.toUpload.incrementAndGet();
                        uploaders.execute(new UploadTask(entry));
                    }
                }
            } else {
                // File is not found. Hash and see where it went.
                filesToHash++;
                hashers.execute(new HasherTask(new FileEntry(cur)));
            }
        }

        // Ready to upload
        hashers.shutdown();
        while (!hashers.isTerminated()) {
            printStatus();
            flush();
            hashers.awaitTermination(500, TimeUnit.MILLISECONDS);
        }
        System.out.println();

        System.out.println("Hashing complete");
        List<FileEntry> nonexistent = manifest.nonexistentFiles();
        if (!nonexistent.isEmpty()) {
            System.out.println("These files are missing:");
            for (FileEntry entry : nonexistent) {
                System.out.println(entry.path);
            }
            System.out.println(nonexistent.size() + " total");
        }
        
        if (!manifest.corruptedFiles().isEmpty()) {
            System.out.println("These files are corrupted:");
            for (FileEntry entry : manifest.corruptedFiles()) {
                System.out.println(entry.path);
            }
            System.out.println(manifest.corruptedFiles().size() + " total");
        }

        uploaders.shutdown();
        while (!uploaders.isTerminated()) {
            printStatus();
            flush();
            uploaders.awaitTermination(500, TimeUnit.MILLISECONDS);
        }
        printStatus();
        flush();
        System.out.println();
        System.out.println("Done!");
    }
}
