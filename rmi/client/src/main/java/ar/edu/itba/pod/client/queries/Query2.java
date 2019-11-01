package ar.edu.itba.pod.client.queries;

import ar.edu.itba.pod.client.utils.PrintResult;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import model.Airport;
import model.Movement;
import org.apache.commons.cli.CommandLine;
import query1.Query1CombinerFactory;
import query1.Query1Mapper;
import query1.Query1ReducerFactory;
import query2.Query2Collator;
import query2.Query2CombinerFactory;
import query2.Query2Mapper;
import query2.Query2ReducerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Query2 extends BaseQuery {

    private final static int id = 2;
    /* This interface defines the IList object that is used throughout Content Server
    for database queries and other representations of tabular data.**/
    private IList<Movement> movements;
    private CommandLine arguments;
    private PrintResult printResult;

    List<queryOutput> qO;

    public Query2(IList<Movement> movements, HazelcastInstance hazelcastInstance, CommandLine arguments,
                  PrintResult printResult) {
        super(hazelcastInstance, arguments);
        this.movements = movements;
        this.arguments = arguments;
        this.printResult = printResult;
    }

    @Override
    public void run() throws ExecutionException, InterruptedException, IOException {
        /* Create Query 2 Job */
        JobTracker jobTracker = getJobTracker();

        /* MapReduce Key Value Source */
        KeyValueSource<String, Movement> source = KeyValueSource.fromList(movements);

        Integer n = Integer.valueOf(arguments.getOptionValue("n"));

        /* MapReduce Creación del Job */
        Job<String, Movement> job = jobTracker.newJob(source);
        ICompletableFuture<List<Map.Entry<String, Double>>> future = job
                .mapper(new Query2Mapper())
                .combiner(new Query2CombinerFactory())
                .reducer(new Query2ReducerFactory())
                .submit(new Query2Collator(n));

        /* Wait and retrieve the result, Airport porcentage result
         * Resultado obtenido por vía sincrónica */
        List<Map.Entry<String, Double>> result = future.get();

        qO = getResult(result);

        /* write file */
        writeResult();

        for(queryOutput q : qO){
            System.out.println(q);
        }
    }

    @Override
    public void writeResult() throws IOException {
        writResult(qO);
    }

    private void writResult(List<queryOutput> results){
        printResult.append("Aerolínea;Porcentaje\n");
        results.forEach(p -> printResult.append(p+"\n"));
    }

    @Override
    public String getResult() {
        StringBuilder builder = new StringBuilder();

        qO.forEach(l -> builder.append(l.airlineName).append(";").append(l.percentage).append("\n"));

        return builder.toString();
    }

    public List<queryOutput> getResult(List<Map.Entry<String, Double>> result){

        List<queryOutput> queryOutputList = new ArrayList<>();

        for(Map.Entry<String, Double> entry : result) {
                queryOutputList.add(new queryOutput(entry.getKey(), entry.getValue()));
        }

        return queryOutputList;
    }

    private class queryOutput{
        String airlineName;
        Double percentage;

        public queryOutput(String airlineName, Double porcentage) {
            this.airlineName = airlineName;
            this.percentage = porcentage;
        }

        public String getAirlineName() {
            return airlineName;
        }

        public Double getPorcentage() {
            return percentage;
        }

        @Override
        public String toString() {
            return airlineName + " ; " + String.format("%1.2f", percentage);
        }
    }
}
