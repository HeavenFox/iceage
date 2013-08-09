package com.zhujingsi.iceage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.annotations.Expose;

public class Manifest {
    @Expose
    private String vaultName;

    @Expose
    private String endpoint;

    private Map<String, FileEntry> pathMap = new HashMap<String, Manifest.FileEntry>();

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    @Expose
    private Map<String, FileEntry> files = new HashMap<String, Manifest.FileEntry>();

    private List<FileEntry> corrupted = new LinkedList<FileEntry>();

    public static class FileEntry {
        @Expose
        public String path;

        @Expose
        public long lastModified;

        @Expose
        public String archiveId;

        public File file;

        public String treeHash;

        private boolean fileExists;
        
        private boolean brandNew;
        
        public boolean isBrandNew() {
            return brandNew;
        }
        
        public FileEntry() {

        }

        public void markExists() {
            fileExists = true;
        }

        public boolean exists() {
            return fileExists;
        }

        public boolean uploaded() {
            return archiveId != null;
        }

        public FileEntry(File fn) {
            this();
            this.file = fn;
            this.path = fn.getPath();
            this.lastModified = fn.lastModified();
            this.markExists();
            this.brandNew = true;
        }

        public void update(FileEntry fileToHash) {
            if (fileToHash.file != null) {
                file = fileToHash.file;
            }

            if (fileToHash.path != null) {
                path = fileToHash.path;
            }

            if (fileToHash.archiveId != null) {
                archiveId = fileToHash.archiveId;
            }

            if (fileToHash.treeHash != null) {
                treeHash = fileToHash.treeHash;
            }

            if (fileToHash.lastModified != 0) {
                lastModified = fileToHash.lastModified;
            }
            
            if (fileToHash.brandNew) {
                brandNew = true;
            }
        }
    }

    private File manifestFile;

    public Manifest() {
    }

    public Manifest(File f) {
        this();
        manifestFile = f;
    }

    public List<FileEntry> nonexistentFiles() {
        List<FileEntry> non = new LinkedList<FileEntry>();
        for (FileEntry f : files.values()) {
            if (!f.exists())
                non.add(f);
        }
        return non;
    }

    public List<FileEntry> corruptedFiles() {
        return corrupted;
    }

    public synchronized void addCorrupted(FileEntry f) {
        corrupted.add(f);
    }

    public static Manifest readManifest(String fn) throws JsonSyntaxException, JsonIOException, FileNotFoundException {
        return readManifest(new File(fn));
    }

    public static Manifest readManifest(File f) throws JsonSyntaxException, JsonIOException, FileNotFoundException {
        Gson json = new Gson();
        Manifest manifest = json.fromJson(new BufferedReader(new FileReader(f)), Manifest.class);
        manifest.manifestFile = f;
        manifest.postDeserialize();
        return manifest;
    }

    private void postDeserialize() {
        for (Entry<String, FileEntry> e : files.entrySet()) {
            String key = e.getKey();
            FileEntry val = e.getValue();
            val.treeHash = key;
            val.file = new File(val.path);
            pathMap.put(val.path, val);
        }
    }

    public FileEntry entryForPath(String path) {
        return pathMap.get(path);
    }
    
    public FileEntry entryForHash(String hash) {
        return files.get(hash);
    }

    public synchronized boolean putEntry(String hash, FileEntry entry) {
        boolean newfile = true;
        if (files.containsKey(hash)) {
            newfile = false;
            pathMap.remove(files.get(hash).path);
        }
        files.put(hash, entry);
        pathMap.put(entry.path, entry);
        return newfile;
    }

    public String getVault() {
        return vaultName;
    }

    public void setVault(String vault) {
        this.vaultName = vault;
    }

    public synchronized void flush() throws IOException {
        Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
        String result = gson.toJson(this);
        FileWriter writer = new FileWriter(manifestFile);
        try {
            writer.write(result);
        } finally {
            writer.close();
        }
    }

    public void removeEntry(FileEntry entry) {
        pathMap.remove(entry.path);
        files.remove(entry.treeHash);
    }
}
