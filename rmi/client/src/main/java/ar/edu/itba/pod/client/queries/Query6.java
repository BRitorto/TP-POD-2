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
import query6.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class Query6 extends BaseQuery {

    private final IList<Airport> airports;
    private final IList<Movement> movements;
    private final CommandLine arguments;
    private final PrintResult printResult;
    private List<queryOutput> queryOutputs;

    public Query6(final IList<Airport> airports, final IList<Movement> movements,
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
        final JobTracker jobTracker = getJobTracker();
        final Map<String, String> oaciProvinceMap = oaciProvinceMap();
        final KeyValueSource<String, Movement> source = KeyValueSource.fromList(movements);
        final Integer min = Integer.valueOf(arguments.getOptionValue("min"));
        final Job<String, Movement> job = jobTracker.newJob(source);
        final ICompletableFuture<Set<Map.Entry<ProvinceTuple, Integer>>> future = job
                .mapper(new Query6Mapper(oaciProvinceMap))
                .combiner(new Query6CombinerFactory())
                .reducer(new Query6ReducerFactory())
                .submit(new Query6Collator(min));
        final Set<Map.Entry<ProvinceTuple, Integer>> result = future.get();
        queryOutputs = getResult(result);
        writeResult();
    }

    private Map<String, String> oaciProvinceMap(){
        final Map<String, String> m = new HashMap<>();
        for(final Airport airport : airports) {
            airport.getOaci().ifPresent(OACI -> m.put(OACI,airport.getProvince()));
        }
        return m;
    }

    @Override
    public void writeResult() {
        writeResult(queryOutputs);
    }

    private void writeResult(final List<queryOutput> results){
        printResult.append("Provincia A;Provincia B;Movimientos\n");
        results.forEach(p -> printResult.append(p+"\n"));
    }

    @Override
    public String getResult() {
        final StringBuilder builder = new StringBuilder();
        queryOutputs.forEach(l -> builder.append(l.provinceA).append(";").append(l.provinceB).
                append(";").append(l.movements).append("\n"));
        return builder.toString();
    }

    private List<queryOutput> getResult(final Set<Map.Entry<ProvinceTuple, Integer>> result){
        final List<queryOutput> queryOutputList = new ArrayList<>();
        for(final Map.Entry<ProvinceTuple, Integer> entry : result) {
                queryOutputList.add(new queryOutput(entry.getKey().getProvince1(), entry.getKey().getProvince2(), entry.getValue()));
        }
        return queryOutputList;
    }

    private class queryOutput {
        private final String provinceA;
        private final String provinceB;
        private final int movements;

        public queryOutput(final String provinceA, final String provinceB, final int movements) {
            this.provinceA = provinceA;
            this.provinceB = provinceB;
            this.movements = movements;
        }

        @Override
        public String toString() {
            return provinceA + ";" + provinceB + ";" + movements;
        }
    }
}
