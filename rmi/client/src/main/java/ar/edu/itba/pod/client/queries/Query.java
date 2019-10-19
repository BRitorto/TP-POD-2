package ar.edu.itba.pod.client.queries;

import java.io.IOException;

/**
 * Created by estebankramer on 19/10/2019.
 */
public interface Query {

    void readData();

    void uploadData();

    void run();

    void writeResult() throws IOException;

    String getResult();


}
