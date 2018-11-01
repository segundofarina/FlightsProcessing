package ar.edu.itba.pod.Query5;

import ar.edu.itba.pod.FlightType;
import ar.edu.itba.pod.Movement;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.Map;
import java.util.Optional;

public class InternationalMapper implements Mapper<String, Movement, String, Integer> {

    /* Key is oaci code, value is iata code */
    private Map<String, String> oaciIataMap;

    public InternationalMapper(Map<String, String> oaciIataMap) {
        this.oaciIataMap = oaciIataMap;
    }

    @Override
    public void map(String s, Movement movement, Context<String, Integer> context) {
        Optional<String> iataOrigin = Optional.ofNullable(oaciIataMap.get(movement.getSourceOASI()));
        Optional<String> iataDestination = Optional.ofNullable(oaciIataMap.get(movement.getDestinationOASI()));

        iataOrigin.ifPresent((key) -> {
            context.emit(key, getValue(movement));
        });

        iataDestination.ifPresent((key) -> {
            context.emit(key, getValue(movement));
        });
    }

    private int getValue(Movement movement) {
        if(movement.getFlightType().filter(f -> f == FlightType.INTERNATIONAL).isPresent() ) {
            return 1;
        }
        //return -1;
        return 0;
    }
}
