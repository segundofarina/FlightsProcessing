package ar.edu.itba.pod.Query1;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class MovementsCombinerFactory implements CombinerFactory<String, Integer, Integer> {
    @Override
    public Combiner<Integer, Integer> newCombiner(String s) {
        return new MovementsCombiner();
    }

    private class MovementsCombiner extends Combiner<Integer, Integer> {
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
