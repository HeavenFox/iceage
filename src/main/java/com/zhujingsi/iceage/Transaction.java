package com.zhujingsi.iceage;

public abstract class Transaction {
    public long timestamp;
    
    public Transaction() {
        timestamp = System.currentTimeMillis();
    }
}
