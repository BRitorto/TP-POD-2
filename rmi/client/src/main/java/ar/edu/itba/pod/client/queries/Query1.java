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
import query1.Query1CombinerFactory;
import query1.Query1Mapper;
import query1.Query1ReducerFactory;
import org.apache.commons.cli.CommandLine;;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class Query1 extends BaseQuery {

    private final IList<Airport> airports;
    private final IList<Movement> movements;
    private final CommandLine arguments;
    private final PrintResult printResult;
    private List<queryOutput> queryOutputs;

    public Query1(final IList<Airport> airports, final IList<Movement> movements,
                  final HazelcastInstance hazelcastInstance, final CommandLine arguments,
                  final PrintResult printResult) {
        super(hazelcastInstance, arguments);
        this.airports = airports;
        this.movements = movements;
        this.arguments = arguments;
        this.printResult = printResult;
    }

    @Override
    public void run() throws ExecutionException, InterruptedException {
        /* Create Query 1 Job */
        final JobTracker jobTracker = getJobTracker();
        /* MapReduce Key Value Source */
        final KeyValueSource<String, Movement> source = KeyValueSource.fromList(movements);
        /* MapReduce Job Creation */
        final Job<String, Movement> job = jobTracker.newJob(source);
        final ICompletableFuture<Map<String, Integer>> future = job
                .mapper(new Query1Mapper())
                .combiner(new Query1CombinerFactory())
                .reducer(new Query1ReducerFactory())
                .submit();
        final Map<String, Integer> result = future.get();
        queryOutputs = getResult(result);
        writeResult();
    }

    private Map<String, String> oaciNameMap(){
        final Map<String, String> m = new HashMap<>();
        for(final Airport airport : airports) {
            airport.getOaci().ifPresent(OACI -> m.put(OACI,airport.getName()));
        }
        return m;
    }

    @Override
    public void writeResult() {
        writeResult(queryOutputs);
    }

    private void writeResult(final List<queryOutput> results){
        printResult.append("OACI;Denominación;Movimientos\n");
        results.forEach(p -> printResult.append(p+"\n"));
    }
    
    @Override
    public String getResult() {
        final StringBuilder builder = new StringBuilder();
        queryOutputs.forEach(l -> builder.append(l.OACI).append(";").append(l.name).append(";").append(l.sum).append("\n"));
        return builder.toString();
    }

    private List<queryOutput> getResult(final Map<String, Integer> result){
        final List<queryOutput> queryOutputList = new ArrayList<>();
        final Map<String, String> oaciNameMap = oaciNameMap();

        for(final String OACI : result.keySet()) {
            final String name = oaciNameMap.get(OACI);
            if(name != null) {
                queryOutputList.add(new queryOutput(OACI, name, result.get(OACI)));
            }
        }
        queryOutputList.sort(Comparator.comparing(queryOutput::getSum).reversed().thenComparing(queryOutput::getOACI));
        return queryOutputList;
    }

    private static class queryOutput{
        String OACI;
        String name;
        int sum;

        public queryOutput(final String OACI, final String name, final Integer sum) {
            this.OACI = OACI;
            this.name = name;
            this.sum = sum;
        }

        public String getOACI() {
            return OACI;
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
