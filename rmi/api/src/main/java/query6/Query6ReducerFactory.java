package query6;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

/* MapReduce Reducer */
public class Query6ReducerFactory implements ReducerFactory<ProvinceTuple, Integer, Integer> {

    @Override
    public Reducer<Integer, Integer> newReducer(ProvinceTuple p) {
        return new query6.Query6ReducerFactory.Query6Reducer();
    }

    private class Query6Reducer extends Reducer<Integer, Integer>{

        private volatile int sum = 0;

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