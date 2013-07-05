package com.zhujingsi.iceage;

public class FlusherThread extends Thread
{
    public void run() {
        try {
            while (true)
            {
                Thread.sleep(1000);
            }
                
        } catch (InterruptedException e) {
            System.out.println("interrupted");
        }
    }

}
