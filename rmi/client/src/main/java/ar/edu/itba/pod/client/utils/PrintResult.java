package ar.edu.itba.pod.client.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;


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

   public void appendTimeOf(String methodName, String className, int lineNumber, String data){
       LocalDate date = LocalDate.now();
       DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/YYYY");
       String strDate = formatter.format(date);

       append(strDate + " " + LocalTime.now() + " INFO [" + methodName + "] " + className +
               " (" + className + ".java:" + lineNumber + ") - " + data +"\n");
   }

}

