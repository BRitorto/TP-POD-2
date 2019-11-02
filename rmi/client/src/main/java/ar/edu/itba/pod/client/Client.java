package ar.edu.itba.pod.client;

import ar.edu.itba.pod.client.queries.*;
import ar.edu.itba.pod.client.utils.AirportCsvParser;
import ar.edu.itba.pod.client.utils.CsvParser;
import ar.edu.itba.pod.client.utils.MovementCsvParser;
import ar.edu.itba.pod.client.utils.PrintResult;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import model.Airport;
import model.Movement;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    private static final String QUERY_OPTION_NAME = "query";
    private static final String QUERY_OPTION_DESCRIPTION = "Query number to run [1-6]";

    private static final String ADDRESSES_NAME = "addresses";
    private static final String ADDRESSES_DESCRIPTION = "Address to start the server on";

    private static final String IN_PATH_NAME = "inPath";
    private static final String IN_PATH_DESCRIPTION = "Path where the data .csv is located";

    private static final String OUT_PATH_NAME = "outPath";
    private static final String OUT_PATH_DESCRIPTION = "Path to leave the out files in";

    private static final String N_NAME = "n";
    private static final String N_DESCRIPTION = "Extra parameter to delimit number of results";

    private static final String OACI_NAME = "oaci";
    private static final String OACI_DESCRIPTION = "OACI of the airport";

    private static final String MIN_NAME = "min";
    private static final String MIN_DESCRIPTION = "Min";
//    TODO add descriptions


    public static void main( String[] args ) throws ExecutionException, InterruptedException, IOException {
        Options options;
        options = new Options();
        addOption(options, QUERY_OPTION_NAME, QUERY_OPTION_DESCRIPTION, true, true);
        addOption(options, ADDRESSES_NAME, ADDRESSES_DESCRIPTION, true, true);
        addOption(options, IN_PATH_NAME, IN_PATH_DESCRIPTION, true, true);
        addOption(options, OUT_PATH_NAME, OUT_PATH_DESCRIPTION, true, true);
        addOption(options, N_NAME, N_DESCRIPTION, true, false);
        addOption(options, OACI_NAME, OACI_DESCRIPTION, true, false);
        addOption(options, MIN_NAME, MIN_DESCRIPTION, true, false);

        /* Retrieve arguments info */
        CommandLine commandLine = parse(args, options);

        /* Set configuration for Hazelcast client */
        ClientConfig clientConfig = getConfig(commandLine);

        int queryNumber = Integer.parseInt(commandLine.getOptionValue(QUERY_OPTION_NAME));

        logger.info("Client starting ...");

        /* Hazelcast client instance*/
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        /* Class info for time file */
        String className = Client.class.getSimpleName();
        String methodName = new Exception().getStackTrace()[0].getMethodName();

        /* Result & Time file */
        PrintResult printResult = new PrintResult(commandLine.getOptionValue("outPath") + "query"+queryNumber+".csv");
        PrintResult printTime = new PrintResult(commandLine.getOptionValue("outPath") + "query"+queryNumber+".txt");

        printTime.appendTimeOf(methodName, className, new Exception().getStackTrace()[0].getLineNumber(),
                "Inicio de la lectura del archivo de entrada");

        /* Read airports file */
        IList<Airport> airportsHz = client.getList("airports");
        CsvParser airportCsvParser = new AirportCsvParser(airportsHz);
        Path airportsPath = Paths.get(commandLine.getOptionValue(IN_PATH_NAME) + "/aeropuertos.csv");
        airportCsvParser.loadData(airportsPath);

        /* Read movements file */
        IList<Movement> movementsHz = client.getList("movements");
        CsvParser movementCsvParser = new MovementCsvParser(movementsHz);
        Path movementsPath = Paths.get(commandLine.getOptionValue(IN_PATH_NAME) + "/movimientos.csv");
        movementCsvParser.loadData(movementsPath);

        printTime.appendTimeOf(methodName, className, new Exception().getStackTrace()[0].getLineNumber(),
                "Fin de lectura del archivo de entrada ");

        logger.info("Running Query #{}", queryNumber);

        /* Create query */
        Query runner = runQuery(queryNumber, airportsHz, movementsHz, client, commandLine, printResult);
        printTime.appendTimeOf(methodName, className, new Exception().getStackTrace()[0].getLineNumber(),
                "Inicio de un trabajo MapReduce");

        /* Run query */
        runner.run();
        printTime.appendTimeOf(methodName, className, new Exception().getStackTrace()[0].getLineNumber(),
                "Fin de un trabajo MapReduce");
//        runner.writeResult();



        /* Close files */
        printResult.close();
        printTime.close();
        airportsHz.destroy();
        movementsHz.destroy();

        /* End client */
        logger.info("Client shutting down ...");
        client.shutdown();
    }

    private static ClientConfig getConfig(CommandLine commandLine) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setGroupConfig(new GroupConfig("g3", "g3"));
        String addresses = commandLine.getOptionValue("addresses");
        String addressesList[] = addresses.split(";");
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        for(String address : addressesList) {
            networkConfig.addAddress(address);
        }
        return clientConfig;
    }

    public static CommandLine parse(String[] args, Options options){
        CommandLineParser parser = new DefaultParser();

        try {
            return parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println( "Parsing failed.  Reason: " + e.getMessage());
            System.exit(1);
            return null;
        }
    }

    public static void addOption(Options options, final String name, final String description, final boolean hasArguments, final boolean required) {
        Option op = new Option(name, name, hasArguments, description);
        op.setRequired(required);
        options.addOption(op);
    }

    private static Query runQuery(int queryNumber, IList<Airport> airports, IList<Movement> movements, HazelcastInstance hazelcastInstance, CommandLine arguments,
                                  PrintResult printResult) {
        switch(queryNumber) {
            case 1:
                return new Query1(airports, movements, hazelcastInstance, arguments, printResult);
            case 2:
                checkParameterN(arguments);
                return new Query2(movements, hazelcastInstance, arguments, printResult);
            case 3:
                return new Query3(movements, hazelcastInstance, arguments, printResult);
            case 4:
                checkParameterN(arguments);
                return new Query4(movements, hazelcastInstance, arguments, printResult);
            case 5:
                return new Query5(movements, hazelcastInstance, arguments, printResult, airports);
            case 6:
                Optional<String> min = Optional.ofNullable(arguments.getOptionValue(MIN_NAME));
                if(!min.isPresent()){
                    throw new IllegalArgumentException("Missing argument min");
                }
                return new Query6(airports, movements, hazelcastInstance, arguments, printResult);
            default:
                throw new IllegalArgumentException("Invalid query number " + queryNumber + ". Insert a value from 1 to 6.");
        }
    }

    private static void checkParameterN(final CommandLine arguments) {
        final Optional<String> n = Optional.ofNullable(arguments.getOptionValue(N_NAME));
        if (!n.isPresent()) {
            throw new IllegalArgumentException("Missing argument N");
        }
    }
}
