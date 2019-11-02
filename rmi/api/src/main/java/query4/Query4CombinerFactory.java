package query4;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

/* MapReduce Combiner */
public class Query4CombinerFactory implements CombinerFactory<String, Integer, Integer> {

    @Override
    public Combiner<Integer, Integer> newCombiner(String s) {
        return new Query4Combiner();
    }

    private static class Query4Combiner extends Combiner<Integer, Integer>{
        private int sum = 0;

        @Override
        public void combine(Integer value) {
            sum += value;
        }

        @Override
        public Integer finalizeChunk() {
            return sum;
        }

        public void reset(){
            sum = 0;
        }
    }
}