package query3;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

/* MapReduce - Mapper */
public class Query3Mapper implements Mapper<String, Integer, Integer, String> {

    public void map(String key, Integer movements, Context<Integer, String> context) {

        /* Group defined by the amount of thousand of movements */
        int groupOfMovements = (movements / 1000) * 1000;

        /* The last posible group to show is 1000 */
        if(groupOfMovements > 0){
            context.emit(groupOfMovements, key);
        }
    }
}
