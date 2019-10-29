package ar.edu.itba.pod.client.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;

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
       if (this.bufferedWriter != null) try {
           this.bufferedWriter.flush();
           this.bufferedWriter.close();
       } catch (IOException ioe2) { }
   }

   public void appendToFile(String data) {
       try {
           this.bufferedWriter.write(data);
       } catch (IOException ioe) {
           ioe.printStackTrace();
       }
   }

//   public void log(String data){
//            appendToFile(LocalDateTime.now() +" INFO - "+data +"\n");
//   }

   public void flush() {
       try {
           this.bufferedWriter.flush();
       } catch (IOException e) {
           e.printStackTrace();
       }
   }

}

