package ar.edu.itba.pod.client.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;


public class PrintResult {

    BufferedWriter bufferedWriter;

   public PrintResult(String name) {
       try {
           bufferedWriter = new BufferedWriter(new FileWriter(name , false));
       } catch (IOException e) {
           e.printStackTrace();
       }
   }

   public void close() {
       if (bufferedWriter != null) try {
           bufferedWriter.flush();
           bufferedWriter.close();
       } catch (IOException ioe2) { }
   }

   public void append(String data) {
       try {
           bufferedWriter.write(data);
       } catch (IOException ioe) {
           ioe.printStackTrace();
       }
   }

}

