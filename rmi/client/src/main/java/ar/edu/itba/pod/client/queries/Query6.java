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

    private IList<Airport> airports;
    private IList<Movement> movements;
    private CommandLine arguments;
    private PrintResult printResult;

    List<queryOutput> qO;

    public Query6(IList<Airport> airports, IList<Movement> movements, HazelcastInstance hazelcastInstance, CommandLine arguments,
                  PrintResult printResult) {
        super(hazelcastInstance, arguments);
        this.airports = airports;
        this.movements = movements;
        this.arguments = arguments;
        this.printResult = printResult;
    }

    @Override
    public void run() throws ExecutionException, InterruptedException {

        /* Create Query 1 Job */
        JobTracker jobTracker = getJobTracker();

        Map<String, String> oaciProvinceMap = oaciProvinceMap();

        /* MapReduce Key Value Source */
        KeyValueSource<String, Movement> source = KeyValueSource.fromList(movements);

        Integer min = Integer.valueOf(arguments.getOptionValue("min"));

        /* MapReduce Job Creation */
        Job<String, Movement> job = jobTracker.newJob(source);
        ICompletableFuture<Set<Map.Entry<ProvinceTuple, Integer>>> future = job
                .mapper(new Query6Mapper(oaciProvinceMap))
                .combiner(new Query6CombinerFactory())
                .reducer(new Query6ReducerFactory())
                .submit(new Query6Collator(min));

        /* Wait and retrieve the result, OACI movement result */
        Set<Map.Entry<ProvinceTuple, Integer>> result = future.get();

        qO = getResult(result);

        /* write file */
        writeResult();
    }

    private Map<String, String> oaciProvinceMap(){
        Map<String, String> m = new HashMap<>();

        for(Airport airport : airports) {
            airport.getOaci().ifPresent(OACI -> m.put(OACI,airport.getProvince()));
        }

        return m;
    }

    @Override
    public void writeResult() {
        writResult(qO);
    }

    private void writResult(List<queryOutput> results){
        printResult.append("Provincia A;Provincia B;Movimientos\n");
        results.forEach(p -> printResult.append(p+"\n"));
    }

    @Override
    public String getResult() {
        StringBuilder builder = new StringBuilder();

        qO.forEach(l -> builder.append(l.provinceA).append(";").append(l.provinceB).
                append(";").append(l.movements).append("\n"));

        return builder.toString();
    }

    public List<queryOutput> getResult(Set<Map.Entry<ProvinceTuple, Integer>> result){

        List<queryOutput> queryOutputList = new ArrayList<>();

        for(Map.Entry<ProvinceTuple, Integer> entry : result) {
                queryOutputList.add(new queryOutput(entry.getKey().getProvince1(), entry.getKey().getProvince2(), entry.getValue()));
        }

        Collections.sort(queryOutputList);

        return queryOutputList;
    }

    private class queryOutput implements Comparable<queryOutput>{
        String provinceA;
        String provinceB;
        int movements;

        public queryOutput(String provinceA, String provinceB, int movements) {
            this.provinceA = provinceA;
            this.provinceB = provinceB;
            this.movements = movements;
        }

        public String getProvinceA() {
            return provinceA;
        }

        public String getProvinceB() {
            return provinceB;
        }

        public int getMovements() {
            return movements;
        }

        @Override
        public String toString() {
            return provinceA + ";" + provinceB + ";" + movements;
        }

        @Override
        public int compareTo(queryOutput queryOutput) {
            return queryOutput.movements - movements;
        }
    }
}
