package ar.edu.itba.pod.client.utils;

import com.hazelcast.core.IList;
import model.Airport;
import model.FlightEnum;
import model.Movement;
import model.MovementEnum;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class MovementCsvParser implements CsvParser {

    private final IList<Movement> movementsHz;
    private final List<Movement> localMovements;

    public MovementCsvParser(IList<Movement> movementsHz){
        this.movementsHz = movementsHz;
        this.localMovements = new ArrayList<>();
    }


    @Override
    public void loadData(Path path) {
        try(Stream<String> stream = Files.lines(path)) {
            /* Skip the first line: labels */
            stream.skip(1).forEach(this::getMovementData);
            /* Add data to IList */
            movementsHz.addAll(localMovements);
        } catch (IOException e) {
            System.out.println("Unable to load airports");
        }
    }

    private void getMovementData(String line) {
        String[] column = line.split(";");
        /* Info we need for queries */
        localMovements.add(new Movement(flightType(column[3]), movementType(column[4]), column[5], column[6]));
    }

    private Optional<FlightEnum> flightType(String s) {
        if(s.equalsIgnoreCase("cabotaje")) {
            return Optional.of(FlightEnum.LOCAL);
        }
        if(s.equalsIgnoreCase("internacional")) {
            return Optional.of(FlightEnum.INTERNATIONAL);
        }
        return Optional.empty();
    }

    private MovementEnum movementType(String s) {
        if(s.equalsIgnoreCase("despegue")) {
            return MovementEnum.DEPARTURE;
        }

        if(s.equalsIgnoreCase("aterrizaje")) {
            return MovementEnum.LANDING;
        }

        throw new IllegalArgumentException("Illegal Movement Type: " + s);
    }
}
