package ar.edu.itba.pod.client.queries;


import ar.edu.itba.pod.query1.*;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Query1 implements Query{

    private final static int id = 1;
    /* This interface defines the IList object that is used throughout Content Server
    for database queries and other representations of tabular data.**/
    private IList<Airport> airports;
    private IList<Movement> movements;
    private BaseQuery baseQuery;

    List<queryOutput> queryOutputs;

    public Query1(IList<Airport> airports, IList<Movement> movements, BaseQuery baseQuery) {
        this.airports = airports;
        this.movements = movements;
        this.baseQuery = baseQuery;
    }

    @Override
    public void run() throws ExecutionException, InterruptedException {
        /* Create Query 1 Job */
        JobTracker jobTracker = this.baseQuery.getJobTracker();

        /* MapReduce Key Value Source */
        KeyValueSource<String, Movement> source = KeyValueSource.fromList(movements);

        /* MapReduce Creación del Job */
        Job<String, Movement> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Integer>> future = job
                .mapper(new Query1Mapper())
                .combiner(new Query1CombinerFactory())
                .reducer(new Query1ReducerFactory())
                .submit();

        /* Wait and retrieve the result, OACI movement result
        * Resultado obtenido por vía sincrónica */
        Map<String, Integer> result = future.get();

        queryOutputs = getResult(result);



    }

    private Map<String, String> oaciNameMap(){
        Map<String, String> m = new HashMap<>();

        for(Airport airport : airports){
            String oaci = airport.getOaci();
            if (oaci != null) {
                m.put(oaci, airport.getName());
            }
        }
        return m;
    }


    @Override
    public void readData() {

    }

    @Override
    public void uploadData() {

    }

    @Override
    public void writeResult() throws IOException {

    }
    
    @Override
    public String getResult() {
        StringBuilder builder = new StringBuilder();

        queryOutputs.forEach(l -> builder.append(l.OACI).append(";").append(l.name).
                append(";").append(l.sum).append("\n"));

        return builder.toString();
    }

    public List<queryOutput> getResult(Map<String, Integer> result){

        List<queryOutput> queryOutputList = new ArrayList<>();
        Map<String, String> oaciNameMap = oaciNameMap();

        for(String oaci : result.keySet()) {
            String name = oaciNameMap.get(oaci);
            if(name != null) {
                queryOutputList.add(new queryOutput(oaci, name, result.get(oaci)));
            }
        }

        /* sort result */
        queryOutputList.sort(Comparator.comparing(queryOutput::getSum).reversed().
                thenComparing(queryOutput::getOACI));

        return queryOutputList;
    }

    private class queryOutput{
        String OACI;
        String name;
        int sum;

        public queryOutput(String OACI, String name, Integer sum) {
            this.OACI = OACI;
            this.name = name;
            this.sum = sum;
        }

        public String getOACI() {
            return OACI;
        }

        public String getName() {
            return name;
        }

        public int getSum() {
            return sum;
        }

        @Override
        public String toString() {
            return OACI + ";" + name + ";" + sum;
        }
    }
}
