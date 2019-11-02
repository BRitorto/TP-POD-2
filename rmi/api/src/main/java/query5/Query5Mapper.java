package query5;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import model.FlightClassEnum;
import model.Movement;

import java.util.Set;

public class Query5Mapper implements Mapper<String, Movement, String, Boolean>  {


    private final Set<String> airports;

    public Query5Mapper(Set<String> airports) {
        this.airports = airports;
    }

    @Override
    public void map(String key, Movement movement, Context<String, Boolean> context) {

        // Emit OACI designator and a boolean describing whether or not a flight is private
        if(airports.contains(movement.getStartOACI())) {
            context.emit(movement.getStartOACI(), isPrivateFlight(movement));
        }
        if(airports.contains(movement.getEndOACI())) {
            context.emit(movement.getEndOACI(), isPrivateFlight(movement));
        }
    }

    private boolean isPrivateFlight(final Movement movement) {
        return movement.getFlightClass().equals(FlightClassEnum.PRIVATE);
    }
}
