package query5;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query5ReducerFactory implements ReducerFactory<String, Long[], Double> {

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
            privateFlight += flights[0];
            total += flights[1];
        }

        @Override
        public Double finalizeReduce() {
            return Math.floor((privateFlight * 100 / (double) total) * 100) / 100;
        }
    }
}
