package ar.edu.itba.pod.Query2;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class ThousandMovementsMapper implements Mapper<String, Integer, Integer, String> {
    @Override
    public void map(String oaci, Integer movements, Context<Integer, String> context) {
        int thousandOfMovements = (movements / 1000) * 1000;

        if(thousandOfMovements > 0) {
            context.emit(thousandOfMovements, oaci);
        }
    }
}
