package query5;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query5ReducerFactory implements ReducerFactory<String, Long[], Double> {

    @Override
    public Reducer<Long[], Double> newReducer(String key) {
        return new Query5Reducer();
    }

    private static class Query5Reducer extends Reducer<Long[], Double> {
        private static final int PRIVATE_MOVEMENTS = 0;
        private static final int TOTAL_MOVEMENTS = 1;
        private volatile int totalMovements;
        private volatile int privateMovements;

        @Override
        public void beginReduce() {
            totalMovements = privateMovements = 0;
        }

        @Override
        public void reduce(Long[] flights) {
            privateMovements += flights[PRIVATE_MOVEMENTS];
            totalMovements += flights[TOTAL_MOVEMENTS];
        }

        @Override
        public Double finalizeReduce() {
            return Math.floor((privateMovements * 100 / (double) totalMovements) * 100) / 100;
        }
    }
}
