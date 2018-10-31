package ar.edu.itba.pod;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

public class Airport implements DataSerializable {
    //private final Optional<String> oaci;
    private String oaci;
    //private final Optional<String> iata;
    private String iata;
    private String name;
    private String state;


    public Airport(){

    }
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

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(oaci);
        out.writeUTF(iata);
        out.writeUTF(name);
        out.writeUTF(state);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        oaci = in.readUTF();
        iata = in.readUTF();
        name = in.readUTF();
        state = in.readUTF();
    }
}
