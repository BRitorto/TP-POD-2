package ar.edu.itba.pod.client.queries;

import ar.edu.itba.pod.client.utils.PrintResult;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import model.Airport;
import model.FlightClassEnum;
import model.Movement;
import org.apache.commons.cli.CommandLine;
import query1.Query1CombinerFactory;
import query1.Query1Mapper;
import query1.Query1ReducerFactory;
import query2.Query2Collator;
import query5.Query5Collator;
import query5.Query5CombinerFactory;
import query5.Query5Mapper;
import query5.Query5ReducerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Query5 extends BaseQuery {

    private final static int id = 5;
    private IList<Movement> movements;
    private Set<String> airports;
    private CommandLine arguments;
    private PrintResult printResult;

    List<queryOutput> qO;

    public Query5(IList<Movement> movements, HazelcastInstance hazelcastInstance, CommandLine arguments,
                  PrintResult printResult, IList<Airport> airports) {
        super(hazelcastInstance, arguments);
        this.airports = new HashSet<>();
        for(Airport airport : airports){
            if(airport.getOaci().isPresent()){
                this.airports.add(airport.getOaci().get());
            }
        }
        this.movements = movements;
        this.arguments = arguments;
        this.printResult = printResult;
    }

    public static int getId() {
        return id;
    }

    public IList<Movement> getMovements() {
        return movements;
    }

    public CommandLine getArguments() {
        return arguments;
    }

    public PrintResult getPrintResult() {
        return printResult;
    }

    public Set<String> getAirports() {
        return airports;
    }

    @Override
    public void run() throws ExecutionException, InterruptedException, IOException {

        /* Create Query 5 Job */
        JobTracker jobTracker = getJobTracker();

        /* MapReduce Key Value Source */
        KeyValueSource<String, Movement> source = KeyValueSource.fromList(movements);

        Integer n = Integer.valueOf(arguments.getOptionValue("n"));

        /* MapReduce Job Creation */
        Job<String, Movement> job = jobTracker.newJob(source);
        ICompletableFuture<List<Map.Entry<String, Double>>> future = job
                .mapper(new Query5Mapper(airports))
                .combiner(new Query5CombinerFactory())
                .reducer(new Query5ReducerFactory())
                .submit(new Query5Collator(n));

        /* Wait and retrieve the result, OACI movement result */
        List<Map.Entry<String, Double>> result = future.get();

        qO = getResult(result);

        /* write file */
//        writeResult();

        for(queryOutput q : qO){
            System.out.println(q);
        }

    }

    @Override
    public String getResult() {

        StringBuilder builder = new StringBuilder();

        qO.forEach(l -> builder.append(l.OACI).append(";").append(l.percentage).append("\n"));

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
        String OACI;
        Double percentage;

        public queryOutput(String OACI, Double porcentage) {
            this.OACI = OACI;
            this.percentage = porcentage;
        }

        public String getOACI() {
            return OACI;
        }

        public Double getPorcentage() {
            return percentage;
        }

        @Override
        public String toString() {
            return OACI + " ; " + String.format("%.2f", percentage);
        }
    }
}
