package ar.edu.itba.pod;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Optional;

public class Airport implements DataSerializable {
    private Optional<String> oaci;
    private Optional<String> iata;
    private String name;
    private String city;


    public Airport(){

    }
    public Airport(Optional<String> oaci, Optional<String> iata, String name, String city) {
        this.oaci = oaci;
        this.iata = iata;
        this.name = name;
        this.city = city;
    }

    public Optional<String> getOaci() {
       return oaci;
    }

    public Optional<String> getIata() {
        return iata;
    }

    public String getName() {
        return name;
    }

    public String getCity() {
        return city;
    }

    @Override
    public String toString() {
        return "Airport{" +
                "oaci=" + oaci +
                ", iata=" + iata +
                ", name='" + name + '\'' +
                ", city='" + city + '\'' +
                '}';
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(oaci.orElse("NULL"));
        out.writeUTF(iata.orElse("NULL"));
        out.writeUTF(name);
        out.writeUTF(city);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        oaci = Optional.of(in.readUTF()).filter(s -> !"NULL".equals(s));
        iata = Optional.of(in.readUTF()).filter(s -> !"NULL".equals(s));
        name = in.readUTF();
        city = in.readUTF();
    }
}
