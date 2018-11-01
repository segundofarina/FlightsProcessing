package ar.edu.itba.pod.Query6;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class CitiesMovementsCombinerFactory implements CombinerFactory<CitiesTuple, Integer, Integer> {
    @Override
    public Combiner<Integer, Integer> newCombiner(CitiesTuple citiesTuple) {
        return new CitiesMovementsCombiner();
    }

    private class CitiesMovementsCombiner extends Combiner<Integer, Integer> {
        private int sum = 0;

        @Override
        public void combine(Integer integer) {
            sum += integer;
        }

        @Override
        public Integer finalizeChunk() {
            return sum;
        }

        @Override
        public void reset() {
            sum = 0;
        }
    }
}
