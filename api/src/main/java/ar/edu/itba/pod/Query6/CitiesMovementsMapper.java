package ar.edu.itba.pod.Query6;

import ar.edu.itba.pod.Movement;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.Map;

public class CitiesMovementsMapper implements Mapper<String, Movement, CitiesTuple, Integer> {
    /* Key is oaci code, value is city name */
    private final Map<String, String> oaciCityMap;

    public CitiesMovementsMapper(Map<String, String> oaciCityMap) {
        this.oaciCityMap = oaciCityMap;
    }

    @Override
    public void map(String s, Movement movement, Context<CitiesTuple, Integer> context) {
        /* Get origin city */
        String originCity = oaciCityMap.get(movement.getSourceOASI());

        /* Get destination city */
        String destinationCity = oaciCityMap.get(movement.getDestinationOASI());

        if(originCity != null && destinationCity != null && !originCity.equalsIgnoreCase(destinationCity)) {
            context.emit(new CitiesTuple(originCity, destinationCity), 1);
        }
    }
}
