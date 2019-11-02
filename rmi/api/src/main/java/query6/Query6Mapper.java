package query6;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import model.Movement;
import model.MovementEnum;

import java.util.Map;

/* MapReduce - Mapper */
public class Query6Mapper implements Mapper<String, Movement, ProvinceTuple, Integer> {

    private final Map<String,String> oaciMap;
    private static final int ONE = 1;

    public Query6Mapper(Map<String, String> oaciMap) {
        this.oaciMap = oaciMap;
    }

    public void map(String key, Movement movement, Context<ProvinceTuple, Integer> context) {

        String startProvince = oaciMap.get(movement.getStartOACI());
        String endProvince = oaciMap.get(movement.getEndOACI());

        if(!(startProvince == null || endProvince == null || startProvince.equalsIgnoreCase(endProvince))){
            context.emit(new ProvinceTuple(startProvince,endProvince), ONE);
        }

    }
}
