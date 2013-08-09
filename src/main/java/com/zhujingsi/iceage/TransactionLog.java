package com.zhujingsi.iceage;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.LinkedList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

public class TransactionLog {
    List<RenameTransaction> rename = new LinkedList<RenameTransaction>();
    List<UpdateTransaction> update = new LinkedList<UpdateTransaction>();
    List<EliminateDuplicateTransaction> deduplicate = new LinkedList<EliminateDuplicateTransaction>();
    
    public transient File transactionFile;
    
    public synchronized void append(RenameTransaction t) {
        rename.add(t);
    }
    
    public synchronized void append(UpdateTransaction t) {
        update.add(t);
    }
    
    public static TransactionLog readTransaction(File f) throws JsonSyntaxException, JsonIOException, FileNotFoundException {
        Gson json = new Gson();
        TransactionLog log = json.fromJson(new FileReader(f), TransactionLog.class);
        return log;
    }
    
    public synchronized String toJson() {
        Gson gson = new GsonBuilder().create();
        String result = gson.toJson(this);
        return result;
    }
    
    public void flush(Writer f) throws IOException {
        f.write(this.toJson());
        f.flush();
    }

    public void flush() throws IOException {
        flush(new BufferedWriter(new FileWriter(transactionFile)));
    }

    public void append(EliminateDuplicateTransaction tr) {
        deduplicate.add(tr);
    }
}
