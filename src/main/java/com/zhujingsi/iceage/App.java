package com.zhujingsi.iceage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import com.zhujingsi.iceage.Manifest.FileEntry;


/**
 * Hello world!
 *
 */
public class App 
{
    private static void listFiles(File cur, List<Pattern> ignorePatterns, List<File> files)
    {
        for (File fn : cur.listFiles()) {
            boolean ignore = false;
            for (Pattern pattern : ignorePatterns)
            {
                if (pattern.matcher(fn.getPath()).find()) {
                    System.out.println("Ignoring" + fn.getPath());
                    ignore = true;
                    break;
                }
            }
            if (ignore) {
                continue;
            }
            if (fn.isDirectory()) {
                listFiles(fn, ignorePatterns, files);
            }
            else
            {
                files.add(fn);
            }
        }
    }
    
    public static void main(String[] args) throws InterruptedException
    {
        // Command line options
        Options options = new Options();
        options.addOption("n", false, "Dry run");
        options.addOption("d", true, "Directory");
        options.addOption("h", true, "Num of hashing threads");
        options.addOption("u", true, "Num of uploading threads");
        
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try
        {
            cmd = parser.parse(options, args);
        } catch (ParseException e2)
        {
            System.out.println("Unable to parse options");
        }
        
        ExecutorService hashers = Executors.newFixedThreadPool(5);
        ExecutorService uploaders = Executors.newFixedThreadPool(5);
        Manifest manifest;
        try {
            manifest = Manifest.readManifest("manifest.json");
        } catch (Exception e) {
            System.out.println("Malformed or missing manifest. Creating one.");
            manifest = new Manifest(new File("manifest.json"));
        }
        
        AWSCredentials credentials = null;
        try
        {
            credentials = new PropertiesCredentials(new FileInputStream("AwsCredentials.properties"));
        } catch (FileNotFoundException e1)
        {
            System.out.println("Cannot find AwsCredentials.properties");
            System.exit(1);
        } catch (IOException e1)
        {
            System.out.println("Cannot read AwsCredentials.properties");
            System.exit(1);
        }
        
        
        
        try
        {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            if (manifest.getEndpoint() == null || manifest.getEndpoint().length() == 0) {
                System.out.println("Please enter the endpoint. We'll use us-west-2 by default.");
                String endpoint = reader.readLine();
                if (endpoint.length() == 0)
                {
                    endpoint = "https://glacier.us-west-2.amazonaws.com/";
                }
                manifest.setEndpoint(endpoint);
            }
            
            if (manifest.getVault() == null || manifest.getVault().length() == 0) {
                System.out.println("Please enter the name of the vault you want to store the files. Make sure it has been created:");
                String vaultName = reader.readLine();
                manifest.setVault(vaultName);
            }

            
        } catch (IOException e)
        {
            System.out.println("Failed to read input.");
            System.exit(1);
        }
        
        AmazonGlacierClient client = new AmazonGlacierClient(credentials);
        client.setEndpoint(manifest.getEndpoint());
        
        UploadTask.setClient(client);

        HasherTask.setManifest(manifest);
        HasherTask.setUploaders(uploaders);
        UploadTask.setManifest(manifest);
        
        List<File> files = new LinkedList<File>();
        List<Pattern> ignore = new LinkedList<Pattern>();
        BufferedReader ignoreReader;
        try
        {
            ignoreReader = new BufferedReader(new FileReader("backupignore"));
            
            String line;
            try
            {
                while ((line = ignoreReader.readLine()) != null) {
                    String trimmed = line.trim();
                    try {
                        ignore.add(Pattern.compile(trimmed));
                    } catch (PatternSyntaxException e) {
                        System.out.println("Ignoring malformed pattern " + trimmed);
                    }
                    
                }
            } catch (IOException e1)
            {
                System.out.println("Failed to read ignore file");
            }
        } catch (FileNotFoundException e2)
        {
            // No ignore patterns
        }
        
        listFiles(new File("."), ignore, files);
        
        int numOfFiles = files.size();
        
        for (File f : files) {
            FileEntry entry = new FileEntry(f);
            hashers.execute(new HasherTask(entry));
        }
        
        hashers.shutdown();
        while (!hashers.isTerminated()) {
            System.out.println("Hashed: " + HasherTask.getCount() + "/" + numOfFiles);
            System.out.println("Uploaded: " + UploadTask.getCount() + "/" + numOfFiles);
            try
            {
                manifest.flush();
            } catch (IOException e)
            {
                System.out.println("Cannot flush");
            }
            Thread.sleep(500);
        }
        
        System.out.println("Hashing complete");
        
        uploaders.shutdown();
        while (!uploaders.isTerminated())
        {
            System.out.println("Hashed: " + HasherTask.getCount() + "/" + numOfFiles);
            System.out.println("Uploaded: " + UploadTask.getCount() + "/" + numOfFiles);
            try
            {
                manifest.flush();
            } catch (IOException e)
            {
                System.out.println("Cannot flush");
            }
            Thread.sleep(500);
        }
        try
        {
            manifest.flush();
        } catch (IOException e)
        {
            System.out.println("Cannot flush");
        }
        System.out.println("Done!");
    }
}
