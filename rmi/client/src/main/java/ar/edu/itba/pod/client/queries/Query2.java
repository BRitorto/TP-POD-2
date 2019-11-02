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

    private final static int id = 2;
    /* This interface defines the IList object that is used throughout Content Server
    for database queries and other representations of tabular data.**/
    private final IList<Movement> movements;
    private final CommandLine arguments;
    private final PrintResult printResult;
    private List<queryOutput> qO;

    public Query2(final IList<Movement> movements, final HazelcastInstance hazelcastInstance,
                  final CommandLine arguments, final PrintResult printResult) {
        super(hazelcastInstance, arguments);
        this.movements = movements;
        this.arguments = arguments;
        this.printResult = printResult;
    }

    @Override
    public void run() throws ExecutionException, InterruptedException, IOException {
        /* Create Query 2 Job */
        final JobTracker jobTracker = getJobTracker();
        /* MapReduce Key Value Source */
        final KeyValueSource<String, Movement> source = KeyValueSource.fromList(movements);
        final Integer n = Integer.valueOf(arguments.getOptionValue("n"));
        /* MapReduce Creación del Job */
        final Job<String, Movement> job = jobTracker.newJob(source);
        final ICompletableFuture<List<Map.Entry<String, Double>>> future = job
                .mapper(new Query2Mapper())
                .combiner(new Query2CombinerFactory())
                .reducer(new Query2ReducerFactory())
                .submit(new Query2Collator(n));
        /* Wait and retrieve the result, Airport porcentage result
         * Resultado obtenido por vía sincrónica */
        final List<Map.Entry<String, Double>> result = future.get();
        qO = getResult(result);
        /* write file */
        writeResult();
    }

    @Override
    public void writeResult() throws IOException {
        writResult(qO);
    }

    private void writResult(final List<queryOutput> results){
        printResult.append("Aerolínea;Porcentaje\n");
        results.forEach(p -> printResult.append(p+"\n"));
    }

    @Override
    public String getResult() {
        final StringBuilder builder = new StringBuilder();
        qO.forEach(l -> builder.append(l.airlineName).append(";").append(l.percentage).append("\n"));
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

        public String getAirlineName() {
            return airlineName;
        }
        public Double getPorcentage() {
            return percentage;
        }

        @Override
        public String toString() {
            return airlineName + " ; " + String.format("%.2f", (percentage - 0.005));
        }
    }
}
