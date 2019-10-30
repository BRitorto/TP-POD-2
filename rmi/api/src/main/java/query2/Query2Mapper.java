package query2;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import model.FlightEnum;
import model.Movement;
import model.MovementEnum;

import java.util.Optional;

/* MapReduce - Mapper */
public class Query2Mapper implements Mapper<String, Movement, String, Integer> {

    private static final int ONE = 1;
    private static final int ZERO = 0;
    public static int total = 0;

    public void map(String key, Movement movement, Context<String, Integer> context) {

        Optional<FlightEnum> flightType = movement.getFlightType();

        if(flightType.isPresent()){
            if(flightType.get() == FlightEnum.LOCAL){
                total++;
                context.emit(movement.getAirlineName(), ONE);
            }
            context.emit(movement.getAirlineName(), ZERO);
        }

    }
}
