package ar.edu.itba.pod.client.queries;

import ar.edu.itba.pod.client.utils.PrintResult;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import model.Movement;
import query1.Query1CombinerFactory;
import query1.Query1Mapper;
import query1.Query1ReducerFactory;
import query3.Query3Mapper;
import query3.Query3ReducerFactory;
import org.apache.commons.cli.CommandLine;;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Query3 extends BaseQuery {

    private final static int id = 3;
    /* This interface defines the IList object that is used throughout Content Server
    for database queries and other representations of tabular data.**/
    private IList<Movement> movements;
    private CommandLine arguments;
    private PrintResult printResult;

    List<queryOutput> qO;

    public Query3(IList<Movement> movements, HazelcastInstance hazelcastInstance, CommandLine arguments,
                  PrintResult printResult) {
        super(hazelcastInstance, arguments);
        this.movements = movements;
        this.arguments = arguments;
        this.printResult = printResult;
    }

    @Override
    public void run() throws ExecutionException, InterruptedException, IOException {
        /* Create Query 3 Job */
        JobTracker jobTracker = getJobTracker();

        /* Movements per airport -> OACI:movements */
        Map<String,Integer> airportMovements = getMovements(jobTracker,movements);

        /* Group by thousand of movements -> Movements-Group:OACIs */
        Map<Integer, List<String>> groupOfMovements = getGroupMovements(jobTracker,airportMovements);

        qO = getResult(groupOfMovements);

        writeResult();

        for(queryOutput q : qO){
            System.out.println(q);
        }
    }

    private Map<String, Integer> getMovements(JobTracker jobTracker, IList<Movement> movements) throws ExecutionException, InterruptedException {
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
        return future.get();
    }

    private Map<Integer, List<String>> getGroupMovements(JobTracker jobTracker, Map<String, Integer> airportMovements) throws ExecutionException, InterruptedException {
        IMap<String, Integer> hzAirportMovements = hazelcastInstance.getMap("airportMovements");
        hzAirportMovements.putAll(airportMovements);

        /* MapReduce Key Value Source */
        KeyValueSource<String, Integer> source = KeyValueSource.fromMap(hzAirportMovements);

        /* MapReduce Creación del Job */
        Job<String, Integer> job = jobTracker.newJob(source);
        ICompletableFuture<Map<Integer, List<String>>> future = job
                .mapper(new Query3Mapper())
                .reducer(new Query3ReducerFactory())
                .submit();

        /* Wait and retrieve the result, Group movements result
         * Resultado obtenido por vía sincrónica */
        Map<Integer, List<String>> result = future.get();

        hzAirportMovements.destroy();

        return result;
    }

    @Override
    public void writeResult() throws IOException {
        writResult(qO);
    }

    private void writResult(List<queryOutput> results){
        printResult.append("Grupo ; AeropuertoA ; AeropuertoB\n");
        results.forEach(p -> printResult.append(p+"\n"));
    }

    @Override
    public String getResult() {
        StringBuilder builder = new StringBuilder();

        qO.forEach(l -> builder.append(l.numberOfMovements).append(";").append(l.OACI1).
                append(";").append(l.OACI2).append("\n"));

        return builder.toString();
    }

    public List<queryOutput> getResult(Map<Integer, List<String>> result){

        List<queryOutput> queryOutputList = new ArrayList<>();

        for(Integer movementsAmount : result.keySet()) {
            List<String> oacis = result.get(movementsAmount);
            if(oacis.size() >= 2) {
                oacis.sort(Comparator.naturalOrder());
                for(int i=0; i<oacis.size(); i++){
                    String airportA = oacis.get(i);
                    for(int j=i+1; j<oacis.size(); j++){
                        String airportB = oacis.get(j);
                        queryOutputList.add(new queryOutput(movementsAmount, airportA, airportB));
                    }
                }
            }
        }

        /* sort result */
        queryOutputList.sort(Comparator.naturalOrder());

        return queryOutputList;
    }

    private class queryOutput implements Comparable<queryOutput>{
        int numberOfMovements;
        String OACI1;
        String OACI2;

        public queryOutput(int numberOfMovements, String OACI1, String OACI2) {
            this.numberOfMovements = numberOfMovements;
            this.OACI1 = OACI1;
            this.OACI2 = OACI2;
        }

        public int getNumberOfMovements() {
            return numberOfMovements;
        }

        public String getOACI1() {
            return OACI1;
        }

        public String getOACI2() {
            return OACI2;
        }

        @Override
        public String toString() {
            return numberOfMovements + " ; " + OACI1 + " ; " + OACI2;
        }

        @Override
        public int compareTo(queryOutput queryOutput) {
            return queryOutput.numberOfMovements - this.numberOfMovements;
        }
    }
}