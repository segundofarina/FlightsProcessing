package ar.edu.itba.pod.Query5;

import ar.edu.itba.pod.FlightType;
import ar.edu.itba.pod.Movement;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class InternationalMapper implements Mapper<String, Movement, String, Integer> {



    @Override
    public void map(String s, Movement movement, Context<String, Integer> context) {
        if(movement.getFlightType().filter(f -> f == FlightType.INTERNATIONAL).isPresent() ) {
            context.emit(movement.getSourceOASI(), 1);
            context.emit(movement.getDestinationOASI(),1);
        }else{
            context.emit(movement.getDestinationOASI(),0);
            context.emit(movement.getSourceOASI(), 0);
        }
    }
}
