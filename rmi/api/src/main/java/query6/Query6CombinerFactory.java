package query6;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

/* MapReduce Combiner */
public class Query6CombinerFactory implements CombinerFactory<ProvinceTuple, Integer, Integer> {

    @Override
    public Combiner<Integer, Integer> newCombiner(ProvinceTuple p) {
        return new query6.Query6CombinerFactory.Query6Combiner();
    }

    private class Query6Combiner extends Combiner<Integer, Integer>{
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

