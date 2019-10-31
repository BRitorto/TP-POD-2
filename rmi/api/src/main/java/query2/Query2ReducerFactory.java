package query2;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

/* MapReduce Reducer */
public class Query2ReducerFactory implements ReducerFactory<String, Integer, Integer> {

    @Override
    public Reducer<Integer, Integer> newReducer(String s) {
        return new Query1Reducer();
    }

    private class Query1Reducer extends Reducer<Integer, Integer>{

        private volatile int sum;

        @Override
        public void beginReduce () {
            sum = 0;
        }

        @Override
        public void reduce(Integer integer) {
            sum += integer;
        }

        @Override
        public Integer finalizeReduce() {
            return sum;
        }
    }
}