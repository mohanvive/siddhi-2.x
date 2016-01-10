package org.wso2.siddhi.wrapper.util;

import java.io.IOException;
import java.io.Writer;
import java.text.DecimalFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Utils {
    public static final long GAME_START_TIME_PS = 10753295594424116L;
    public static final long FIRST_HALF_END_TIME_PS = 12557295594424116L;
    public static final long SECOND_HALF_START_TIME_PS = 13086639146403495L;
    public static final long GAME_END_TIME_PS = 14879639146403495L;

    private static DecimalFormat df = new DecimalFormat(".000");

    private static ExecutorService pool =  Executors.newFixedThreadPool(10);

    private static Writer out;
    
    public static void setWriter(Writer out){
        if(Utils.out != null){
            try {
                Utils.out.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        Utils.out = out; 
    }

    
    public static void submit(Runnable runnable){
        pool.submit(runnable);
    }
    
    public static void write(final String line){
        writeNof(line + "\n");
//        submit(new Runnable() {
//            public void run() {
//                writeNof(line);
//                writeNof("\n");
//            }
//        });
    }
    
    public static void writeNof(String line){
        try {
//            System.out.print(line);
            out.write(line);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    
    public static void flush(){
        try {
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
//        submit(new Runnable() {
//            public void run() {
//                try {
//                  out.flush();
//              } catch (IOException e) {
//                  e.printStackTrace();
//              }            }
//        });
    }
    
    public static void main(String[] args) {
        System.out.println((600+1)%600);
    }
    
}
