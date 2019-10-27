package ar.edu.itba.pod.query1;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query1Combiner implements CombinerFactory<String, Integer, Integer> {

    @Override
    public Combiner newCombiner(Object o) {
        return null;
    }


}
