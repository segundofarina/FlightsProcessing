package ar.edu.itba.pod.Query4;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class LandingsAmountReducerFactory implements ReducerFactory<String, Integer, Integer> {
    @Override
    public Reducer<Integer, Integer> newReducer(String s) {
        return new LandingsAmountReducer();
    }

    private class LandingsAmountReducer extends Reducer<Integer, Integer> {
        private volatile int sum;

        @Override
        public void beginReduce() {
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
