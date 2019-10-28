package query1;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import model.Movement;
import model.MovementEnum;

/* MapReduce - Mapper */
public class Query1Mapper implements Mapper<String, Movement, String, Integer> {

    private static final int ONE = 1;

    public void map(String key, Movement movement, Context<String, Integer> context) {

        if(movement.getMovementType() == MovementEnum.DEPARTURE){
            context.emit(movement.getStartOACI(), ONE);
        }

        if(movement.getMovementType() == MovementEnum.LANDING){
            context.emit(movement.getEndOACI(), ONE);
        }
    }
}
