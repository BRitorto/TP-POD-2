package query3;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.ArrayList;
import java.util.List;

/* MapReduce Reducer */
public class Query3ReducerFactory implements ReducerFactory<Integer, String, List<String>> {

    @Override
    public Reducer<String, List<String>> newReducer(Integer integer) {
        return new query3.Query3ReducerFactory.Query3Reducer();
    }

    private class Query3Reducer extends Reducer<String, List<String>>{

        private volatile List<String> airportList;

        @Override
        public void beginReduce () {
            airportList = new ArrayList<>();
        }

        @Override
        public void reduce(String oaci) {
            airportList.add(oaci);
        }

        @Override
        public List<String> finalizeReduce() {
            return airportList;
        }
    }
}
