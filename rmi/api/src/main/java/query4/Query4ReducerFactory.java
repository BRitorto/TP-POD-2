package query4;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

/* MapReduce Reducer */
public class Query4ReducerFactory implements ReducerFactory<String, Integer, Integer> {

    @Override
    public Reducer<Integer, Integer> newReducer(String s) {
        return new Query4Reducer();
    }

    private static class Query4Reducer extends Reducer<Integer, Integer>{
        private Integer sum;

        @Override
        public void beginReduce () {
            sum = 0;
        }

        @Override
        public void reduce(Integer value) {
            sum += value;
        }

        @Override
        public Integer finalizeReduce() {
            return sum;
        }
    }
}
