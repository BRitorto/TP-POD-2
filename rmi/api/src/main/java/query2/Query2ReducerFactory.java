package query2;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

/* MapReduce Reducer */
public class Query2ReducerFactory implements ReducerFactory<String, Integer, Double> {

    @Override
    public Reducer<Integer, Double> newReducer(String s) {
        return new query2.Query2ReducerFactory.Query2Reducer();
    }

    private class Query2Reducer extends Reducer<Integer, Double>{

        private volatile double sum;
        private int total = Query2Mapper.total;

        @Override
        public void beginReduce () {
            sum = 0;
        }

        @Override
        public void reduce(Integer integer) {
            sum += 100.00 * integer/total;
        }

        @Override
        public Double finalizeReduce() {
            return sum;
        }
    }
}
