package ar.edu.itba.pod.client.utils;

import com.hazelcast.core.IList;
import model.Airport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class AirportCsvParser implements CsvParser{

    private final IList<Airport> airportsHz;
    private final List<Airport> localAirports;

    public AirportCsvParser(IList<Airport> airportsHz) {
        this.airportsHz = airportsHz;
        this.localAirports = new ArrayList<>();
    }

    @Override
    public void loadData(Path path) {
        try(Stream<String> stream = Files.lines(path)) {
            /* Skip the first line: labels */
            stream.skip(1).forEach(this::getAirportData);
            /* Add data to IList */
            airportsHz.addAll(localAirports);
        } catch (IOException e) {
            System.out.println("Unable to load airports");
        }
    }

    private void getAirportData(String line) {
        String[] column = line.split(";");
        /* Info we need for queries */
        localAirports.add(new Airport(valueOf(column[1]), removeQuotes(column[4]), removeQuotes(column[21])));
//        if(localAirports.size() > 100) {
//            airportsHz.addAll(localAirports);
//            localAirports.clear();
//        }
    }

    private Optional<String> valueOf(String s) {
        if(s.equals("")) {
            return Optional.empty();
        }
        return Optional.ofNullable(s);
    }

    private String removeQuotes(String s) {
        return s.replaceAll("^\"|\"$", "");
    }



}
