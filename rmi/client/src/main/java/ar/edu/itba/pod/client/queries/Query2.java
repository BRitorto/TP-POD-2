package ar.edu.itba.pod.client.queries;

import ar.edu.itba.pod.client.utils.PrintResult;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import model.Movement;
import org.apache.commons.cli.CommandLine;
import query2.Query2Collator;
import query2.Query2CombinerFactory;
import query2.Query2Mapper;
import query2.Query2ReducerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Query2 extends BaseQuery {

    private final IList<Movement> movements;
    private final CommandLine arguments;
    private final PrintResult printResult;
    private List<queryOutput> queryOutputs;

    public Query2(final IList<Movement> movements, final HazelcastInstance hazelcastInstance,
                  final CommandLine arguments, final PrintResult printResult) {
        super(hazelcastInstance, arguments);
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
                .mapper(new Query2Mapper())
                .combiner(new Query2CombinerFactory())
                .reducer(new Query2ReducerFactory())
                .submit(new Query2Collator(n));
        final List<Map.Entry<String, Double>> result = future.get();
        queryOutputs = getResult(result);
        writeResult();
    }

    @Override
    public void writeResult() {
        writeResult(queryOutputs);
    }

    private void writeResult(final List<queryOutput> results){
        printResult.append("Aerolínea;Porcentaje\n");
        results.forEach(p -> printResult.append(p+"\n"));
    }

    @Override
    public String getResult() {
        final StringBuilder builder = new StringBuilder();
        queryOutputs.forEach(l -> builder.append(l.airlineName).append(";").append(l.percentage).append("\n"));
        return builder.toString();
    }

    private List<queryOutput> getResult(final List<Map.Entry<String, Double>> result){
        final List<queryOutput> queryOutputList = new ArrayList<>();
        for(Map.Entry<String, Double> entry : result) {
            queryOutputList.add(new queryOutput(entry.getKey(), entry.getValue()));
        }
        return queryOutputList;
    }

    private static class queryOutput{
        private final String airlineName;
        private final Double percentage;

        public queryOutput(final String airlineName, final Double percentage) {
            this.airlineName = airlineName;
            this.percentage = percentage;
        }

        @Override
        public String toString() {
            return airlineName + " ; " + String.format("%.2f", (percentage - 0.005));
        }
    }
}
