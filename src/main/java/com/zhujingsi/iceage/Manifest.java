package com.zhujingsi.iceage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.annotations.Expose;

public class Manifest
{
    @Expose
    private String vaultName;
    
    @Expose
    private String endpoint;
    
    public String getEndpoint()
    {
        return endpoint;
    }

    public void setEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
    }

    @Expose
    private Map<String, FileEntry> files;
    
    public static class FileEntry {
        @Expose
        public String path;
        
        @Expose
        public long lastModified;
        
        @Expose
        public String archiveId;
        
        public File file;
        
        public String treeHash;
        
        public FileEntry()
        {
            
        }
        
        public FileEntry(File fn)
        {
            this.file = fn;
            this.path = fn.getPath();
            this.lastModified = fn.lastModified();
        }
    }
    
    private File manifestFile;
    
    public Manifest()
    {
        this.files = new HashMap<String, Manifest.FileEntry>();
    }
    
    public Manifest(File f)
    {
        this();
        manifestFile = f;
    }
    
    public static Manifest readManifest(String fn) throws JsonSyntaxException, JsonIOException, FileNotFoundException
    {
        return readManifest(new File(fn));
    }
    
    public static Manifest readManifest(File f) throws JsonSyntaxException, JsonIOException, FileNotFoundException
    {
        Gson json = new Gson();
        Manifest manifest = json.fromJson(new BufferedReader(new FileReader(f)), Manifest.class);
        manifest.manifestFile = f;
        return manifest;
    }
    
    
    public synchronized boolean addEntry(String hash, FileEntry entry)
    {
        if (files.containsKey(hash)) {
            return false;
        }
        files.put(hash, entry);
        return true;
    }
    
    public String getVault()
    {
        return vaultName;
    }

    public void setVault(String vault)
    {
        this.vaultName = vault;
    }
    
    public synchronized void flush() throws IOException
    {
        Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
        String result = gson.toJson(this);
        FileWriter writer = new FileWriter(manifestFile);
        try
        {
            writer.write(result);
        }
        finally
        {
            writer.close();
        }
    }
}
