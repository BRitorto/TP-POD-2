package query2;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

/* MapReduce Combiner */
public class Query2CombinerFactory implements CombinerFactory<String, Integer, Integer> {

    @Override
    public Combiner<Integer, Integer> newCombiner(String s) {
        return new query2.Query2CombinerFactory.Query2Combiner();
    }

    private class Query2Combiner extends Combiner<Integer, Integer>{
        private int sum = 0;

        @Override
        public void combine(Integer integer) {
            sum += integer;
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