package ar.edu.itba.pod.client.queries;

import ar.edu.itba.pod.client.utils.PrintResult;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import model.Movement;
import org.apache.commons.cli.CommandLine;
import query4.Query4Collator;
import query4.Query4CombinerFactory;
import query4.Query4Mapper;
import query4.Query4ReducerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Query4 extends BaseQuery {
    private static int id = 4;
    private final IList<Movement> movements;
    private final CommandLine arguments;
    private final PrintResult printResult;
    private List<queryOutput> q4;


    public Query4(final IList<Movement> movements, final HazelcastInstance hazelcastInstance,
                  final CommandLine arguments, final PrintResult printResult) {
        super(hazelcastInstance, arguments);
        this.movements = movements;
        this.arguments = arguments;
        this.printResult = printResult;
    }

    @Override
    public void run() throws ExecutionException, InterruptedException, IOException {
        final JobTracker jobTracker = getJobTracker();
        final List<Map.Entry<String, Integer>> airportMovements = getMovements(jobTracker, movements);
        q4 = getResult(airportMovements);
        for(queryOutput q : q4){
            System.out.println(q);
        }
        writeResult();
    }


    @Override
    public void writeResult() {
        writResult(q4);
    }

    private void writResult(List<queryOutput> results){
        printResult.append("OACI;Despegues\n");
        results.forEach(p -> printResult.append(p+"\n"));
    }

    private List<Map.Entry<String, Integer>> getMovements(final JobTracker jobTracker, final IList<Movement> movements)
            throws ExecutionException, InterruptedException {
        final KeyValueSource<String, Movement> source = KeyValueSource.fromList(movements);
        final Job<String, Movement> job = jobTracker.newJob(source);
        final JobCompletableFuture<List<Map.Entry<String, Integer>>> future = job
                .mapper(new Query4Mapper(this.arguments.getOptionValue("oaci")))
                .combiner(new Query4CombinerFactory())
                .reducer(new Query4ReducerFactory())
                .submit(new Query4Collator(Integer.parseInt(this.arguments.getOptionValue("n"))));
        return future.get();
    }

    private List<queryOutput> getResult(List<Map.Entry<String, Integer>> result){
        final List<queryOutput> queryOutputList = new ArrayList<>();
        for(Map.Entry<String, Integer> entry : result) {
            queryOutputList.add(new queryOutput(entry.getKey(), entry.getValue()));
        }
        return queryOutputList;
    }

    @Override
    public String getResult() {
        final StringBuilder builder = new StringBuilder();
        q4.forEach(l -> builder.append(l.OACIDestination).append(";").append(l.numberOfMovements).append("\n"));
        return builder.toString();    }

    private static class queryOutput implements Comparable<queryOutput> {
        final String OACIDestination;
        final int numberOfMovements;

        public queryOutput(final String OACIDestination, final int numberOfMovements) {
            this.OACIDestination = OACIDestination;
            this.numberOfMovements = numberOfMovements;
        }

        @Override
        public int compareTo(final queryOutput queryOutput) {
            return queryOutput.numberOfMovements - this.numberOfMovements;
        }

        @Override
        public String toString() {
            return OACIDestination + ';' + numberOfMovements;
        }
    }
}
