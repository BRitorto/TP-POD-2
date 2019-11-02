package query4;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import model.Movement;

/* MapReduce - Mapper */
public class Query4Mapper implements Mapper<String, Movement, String, Integer> {
    private final String startOACI;
    private static final int ONE = 1;

    public Query4Mapper(final String startOACI){
        this.startOACI = startOACI;
    }
    @Override
    public void map(String s, Movement movement, Context<String, Integer> context) {
        if (movement.getStartOACI().equals(this.startOACI)) {
            context.emit(movement.getEndOACI(), ONE);
        }
    }
}
