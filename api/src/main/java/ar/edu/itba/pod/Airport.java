package ar.edu.itba.pod;

import java.io.Serializable;
import java.util.Optional;

public class Airport implements Serializable {
    //private final Optional<String> oaci;
    private final String oaci;
    //private final Optional<String> iata;
    private final String iata;
    private final String name;
    private final String state;

    //public Airport(Optional<String> oaci, Optional<String> iata, String name, String state) {
    public Airport(String oaci, String iata, String name, String state) {
        this.oaci = oaci;
        this.iata = iata;
        this.name = name;
        this.state = state;
    }

    //public Optional<String> getOaci() {
    public String getOaci() {
        return oaci;
    }

    //public Optional<String> getIata() {
    public String getIata() {
        return iata;
    }

    public String getName() {
        return name;
    }

    public String getState() {
        return state;
    }

    @Override
    public String toString() {
        return "Airport{" +
                "oaci=" + oaci +
                ", iata=" + iata +
                ", name='" + name + '\'' +
                ", state='" + state + '\'' +
                '}';
    }
}
