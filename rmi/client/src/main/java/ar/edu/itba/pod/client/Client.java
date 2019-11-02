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
import model.Airport;
import model.Movement;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
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

    public static void main( String[] args ) throws ExecutionException, InterruptedException, IOException {
        final Options options = new Options();
        addOption(options, QUERY_OPTION_NAME, QUERY_OPTION_DESCRIPTION, true, true);
        addOption(options, ADDRESSES_NAME, ADDRESSES_DESCRIPTION, true, true);
        addOption(options, IN_PATH_NAME, IN_PATH_DESCRIPTION, true, true);
        addOption(options, OUT_PATH_NAME, OUT_PATH_DESCRIPTION, true, true);
        addOption(options, N_NAME, N_DESCRIPTION, true, false);
        addOption(options, OACI_NAME, OACI_DESCRIPTION, true, false);
        addOption(options, MIN_NAME, MIN_DESCRIPTION, true, false);

        /* Retrieve arguments info */
        final CommandLine commandLine = parse(args, options);

        /* Set configuration for Hazelcast client */
        final ClientConfig clientConfig = getConfig(commandLine);
        final int queryNumber = Integer.parseInt(commandLine.getOptionValue(QUERY_OPTION_NAME));
        logger.info("Client starting ...");

        /* Hazelcast client instance*/
        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        /* Class info for time file */
        final String className = Client.class.getSimpleName();
        final String methodName = new Exception().getStackTrace()[0].getMethodName();

        /* Result & Time file */
        final PrintResult printResult = new PrintResult(commandLine.getOptionValue("outPath") + "query"+queryNumber+".csv");
        final PrintResult printTime = new PrintResult(commandLine.getOptionValue("outPath") + "query"+queryNumber+".txt");

        printTime.appendTimeOf(methodName, className, new Exception().getStackTrace()[0].getLineNumber(),
                "Inicio de la lectura del archivo de entrada");

        /* Read airports file */
        final IList<Airport> airportsHz = client.getList("airports");
        final CsvParser airportCsvParser = new AirportCsvParser(airportsHz);
        final Path airportsPath = Paths.get(commandLine.getOptionValue(IN_PATH_NAME) + "/aeropuertos.csv");
        airportCsvParser.loadData(airportsPath);

        /* Read movements file */
        final IList<Movement> movementsHz = client.getList("movements");
        final CsvParser movementCsvParser = new MovementCsvParser(movementsHz);
        final Path movementsPath = Paths.get(commandLine.getOptionValue(IN_PATH_NAME) + "/movimientos.csv");
        movementCsvParser.loadData(movementsPath);

        printTime.appendTimeOf(methodName, className, new Exception().getStackTrace()[0].getLineNumber(),
                "Fin de lectura del archivo de entrada ");
        logger.info("Running Query #{}", queryNumber);

        /* Create query */
        final Query runner = runQuery(queryNumber, airportsHz, movementsHz, client, commandLine, printResult);
        printTime.appendTimeOf(methodName, className, new Exception().getStackTrace()[0].getLineNumber(),
                "Inicio de un trabajo MapReduce");

        /* Run query */
        runner.run();
        printTime.appendTimeOf(methodName, className, new Exception().getStackTrace()[0].getLineNumber(),
                "Fin de un trabajo MapReduce");

        /* Close files */
        printResult.close();
        printTime.close();
        airportsHz.destroy();
        movementsHz.destroy();

        /* End client */
        logger.info("Client shutting down ...");
        client.shutdown();
    }

    private static ClientConfig getConfig(final CommandLine commandLine) {
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.setGroupConfig(new GroupConfig("g3", "g3"));
        final String addresses = commandLine.getOptionValue("addresses");
        final String addressesList[] = addresses.split(";");
        final ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        for(final String address : addressesList) {
            networkConfig.addAddress(address);
        }
        return clientConfig;
    }

    private static CommandLine parse(final String[] args, final Options options){
        final CommandLineParser parser = new DefaultParser();

        try {
            return parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println( "Parsing failed.  Reason: " + e.getMessage());
            System.exit(1);
            return null;
        }
    }

    private static void addOption(final Options options, final String name, final String description,
                                  final boolean hasArguments, final boolean required) {
        final Option op = new Option(name, name, hasArguments, description);
        op.setRequired(required);
        options.addOption(op);
    }

    private static Query runQuery(final int queryNumber, final IList<Airport> airports, final IList<Movement> movements,
                                  final HazelcastInstance hazelcastInstance, final CommandLine arguments,
                                  final PrintResult printResult) {
        switch(queryNumber) {
            case 1:
                return new Query1(airports, movements, hazelcastInstance, arguments, printResult);
            case 2:
                checkExtraParameter(arguments, N_NAME, "Missing argument n");
                return new Query2(movements, hazelcastInstance, arguments, printResult);
            case 3:
                return new Query3(movements, hazelcastInstance, arguments, printResult);
            case 4:
                checkExtraParameter(arguments, N_NAME, "Missing argument n");
                checkExtraParameter(arguments, OACI_NAME, "Missing argument oaci");
                return new Query4(movements, hazelcastInstance, arguments, printResult);
            case 5:
                return new Query5(movements, hazelcastInstance, arguments, printResult, airports);
            case 6:
                checkExtraParameter(arguments, MIN_NAME, "Missing argument min");
                return new Query6(airports, movements, hazelcastInstance, arguments, printResult);
            default:
                throw new IllegalArgumentException("Invalid query number " + queryNumber + ". Insert a value from 1 to 6.");
        }
    }

    private static void checkExtraParameter(final CommandLine arguments, final String paramName, final String message) {
        final Optional<String> parameter = Optional.ofNullable(arguments.getOptionValue(paramName));
        if (!parameter.isPresent()) {
            throw new IllegalArgumentException(message);
        }
    }
}
