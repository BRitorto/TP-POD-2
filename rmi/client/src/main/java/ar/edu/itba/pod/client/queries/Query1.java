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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Query1 extends BaseQuery {

    private final static int id = 1;
    /* This interface defines the IList object that is used throughout Content Server
    for database queries and other representations of tabular data.**/
    private IList<Airport> airports;
    private IList<Movement> movements;
    private CommandLine arguments;
    private PrintResult printResult;

    List<queryOutput> qO;

    public Query1(IList<Airport> airports, IList<Movement> movements, HazelcastInstance hazelcastInstance, CommandLine arguments,
                  PrintResult printResult) {
        super(hazelcastInstance, arguments);
        this.airports = airports;
        this.movements = movements;
        this.arguments = arguments;
        this.printResult = printResult;
    }

    @Override
    public void run() throws ExecutionException, InterruptedException, IOException {
        /* Create Query 1 Job */
        JobTracker jobTracker = getJobTracker();

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

        qO = getResult(result);

        writeResult();

        for(queryOutput q : qO){
            System.out.println(q);
        }
    }

    private Map<String, String> oaciNameMap(){
        Map<String, String> m = new HashMap<>();

        for(Airport airport : airports) {
            airport.getOaci().ifPresent(oaci -> m.put(oaci,airport.getName()));
        }

        return m;
    }

    @Override
    public void writeResult() throws IOException {
        writResult(qO);
    }

    private void writResult(List<queryOutput> results){
        printResult.appendToFile("OACI;Denominación;Movimientos\n");
        results.forEach(p -> printResult.appendToFile(p+"\n"));
    }
    
    @Override
    public String getResult() {
        StringBuilder builder = new StringBuilder();

        qO.forEach(l -> builder.append(l.OACI).append(";").append(l.name).
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
            return OACI + " ; " + name + " ; " + sum;
        }
    }
}
