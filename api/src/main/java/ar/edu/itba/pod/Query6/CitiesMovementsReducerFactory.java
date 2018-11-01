package ar.edu.itba.pod.Query6;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class CitiesMovementsReducerFactory implements ReducerFactory<CitiesTuple, Integer, Integer> {
    @Override
    public Reducer<Integer, Integer> newReducer(CitiesTuple citiesTuple) {
        return new CitiesMovementsReducer();
    }

    private class CitiesMovementsReducer extends Reducer<Integer, Integer> {
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
