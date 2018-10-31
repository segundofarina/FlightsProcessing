package ar.edu.itba.pod.client.Queries;

import ar.edu.itba.pod.Airport;
import ar.edu.itba.pod.Movement;
import com.hazelcast.core.HazelcastInstance;

import java.util.List;

public abstract class BasicQuery {
    private List<Airport> airports;
    private List<Movement> movements;
    private HazelcastInstance hz;

    public BasicQuery(List<Airport> airports, List<Movement> movements, HazelcastInstance hz) {
        this.airports = airports;
        this.movements = movements;
        this.hz = hz;
    }
}
