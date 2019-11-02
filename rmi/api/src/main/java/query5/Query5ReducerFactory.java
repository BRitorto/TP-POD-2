package query5;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query5ReducerFactory implements ReducerFactory<String, Long[], Double> {

    private static final int ONE = 1;
    private static final int ZERO = 0;

    @Override
    public Reducer<Long[], Double> newReducer(String key) {
        return new Query5Reducer();
    }

    private static class Query5Reducer extends Reducer<Long[], Double> {
        private volatile int total;
        private volatile int privateFlight;

        @Override
        public void beginReduce() {
            total = 0;
            privateFlight = 0;
        }

        @Override
        public void reduce(Long[] flights) {
            privateFlight += flights[ZERO];
            total += flights[ONE];
        }

        @Override
        public Double finalizeReduce() {
            return Math.floor((privateFlight * 100 / (double) total) * 100) / 100;
        }
    }
}
