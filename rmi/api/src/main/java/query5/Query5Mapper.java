package query5;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import model.FlightClassEnum;
import model.FlightEnum;
import model.Movement;

import java.util.Optional;

public class Query5Mapper implements Mapper<String, Movement, String, Integer> {

    private static final int ONE = 1;
    private static final int ZERO = 0;
    public static int total = 0;


    @Override
    public void map(String s, Movement movement, Context<String, Integer> context) {

        FlightClassEnum flightclass = movement.getFlightClass();

        if(flightclass == FlightClassEnum.PRIVATE){
//            total++;
            context.emit(movement.getStartOACI(), ONE);
            context.emit(movement.getEndOACI(), ONE);
        }else{
            context.emit(movement.getStartOACI(), ZERO);
            context.emit(movement.getEndOACI(), ZERO);
        }

    }
}
