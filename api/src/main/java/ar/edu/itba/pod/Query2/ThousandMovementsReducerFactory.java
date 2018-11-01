package ar.edu.itba.pod.Query2;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.ArrayList;
import java.util.List;

public class ThousandMovementsReducerFactory implements ReducerFactory<Integer, String, List<String>> {
    @Override
    public Reducer<String, List<String>> newReducer(Integer integer) {
        return new ThousandMovementsReducer();
    }

    private class ThousandMovementsReducer extends Reducer<String, List<String>> {
        private volatile List<String> oaciList;

        @Override
        public void beginReduce() {
            oaciList = new ArrayList<>();
        }

        @Override
        public void reduce(String s) {
            oaciList.add(s);
        }

        @Override
        public List<String> finalizeReduce() {
            return oaciList;
        }
    }
}
