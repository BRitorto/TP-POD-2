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
import query5.Query5Collator;
import query5.Query5CombinerFactory;
import query5.Query5Mapper;
import query5.Query5ReducerFactory;


import java.util.*;
import java.util.concurrent.ExecutionException;

public class Query5 extends BaseQuery {

    private final IList<Movement> movements;
    private final Set<String> airports;
    private final CommandLine arguments;
    private final PrintResult printResult;
    private List<queryOutput> queryOutputs;

    public Query5(final IList<Movement> movements, final HazelcastInstance hazelcastInstance,
                  final CommandLine arguments, final PrintResult printResult, final IList<Airport> airports) {
        super(hazelcastInstance, arguments);
        this.airports = new HashSet<>();
        for(final Airport airport : airports){
            if(airport.getOaci().isPresent()){
                this.airports.add(airport.getOaci().get());
            }
        }
        this.movements = movements;
        this.arguments = arguments;
        this.printResult = printResult;
    }


    @Override
    public void run() throws ExecutionException, InterruptedException {

        final JobTracker jobTracker = getJobTracker();
        final KeyValueSource<String, Movement> source = KeyValueSource.fromList(movements);
        final Integer n = Integer.valueOf(arguments.getOptionValue("n"));
        final Job<String, Movement> job = jobTracker.newJob(source);
        final ICompletableFuture<List<Map.Entry<String, Double>>> future = job
                .mapper(new Query5Mapper(airports))
                .combiner(new Query5CombinerFactory())
                .reducer(new Query5ReducerFactory())
                .submit(new Query5Collator(n));
        final List<Map.Entry<String, Double>> result = future.get();
        queryOutputs = getResult(result);
        writeResult();
    }

    @Override
    public void writeResult(){
        writeResult(queryOutputs);
    }

    private void writeResult(final List<queryOutput> results){
        printResult.append("OACI;Porcentaje\n");
        results.forEach(p -> printResult.append(p+"\n"));
    }

    @Override
    public String getResult() {
        final StringBuilder builder = new StringBuilder();
        queryOutputs.forEach(l -> builder.append(l.OACI).append(";").append(l.percentage).append("\n"));
        return builder.toString();
    }

    private List<queryOutput> getResult(final List<Map.Entry<String, Double>> result){
        final List<queryOutput> queryOutputList = new ArrayList<>();
        for(final Map.Entry<String, Double> entry : result) {
            queryOutputList.add(new queryOutput(entry.getKey(), entry.getValue()));
        }
        return queryOutputList;
    }

    private class queryOutput{
        private final String OACI;
        private final Double percentage;

        public queryOutput(final String OACI, final Double percentage) {
            this.OACI = OACI;
            this.percentage = percentage;
        }

        @Override
        public String toString() {
            return OACI + ";" + String.format(Locale.ROOT, "%.2f", percentage)+"%";
        }
    }
}
