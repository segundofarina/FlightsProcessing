package ar.edu.itba.pod.Query6;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

public class CitiesTuple implements Serializable {
    private String city1;
    private String city2;

    public CitiesTuple(String city1, String city2) {
        this.city1 = city1;
        this.city2 = city2;
    }

    public String getCity1() {
        return city1;
    }

    public String getCity2() {
        return city2;
    }
/*

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(city1);
        out.writeUTF(city2);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        city1 = in.readUTF();
        city2 = in.readUTF();
    }
*/
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CitiesTuple that = (CitiesTuple) o;
        return (Objects.equals(city1, that.city1) && Objects.equals(city2, that.city2)) ||
                (Objects.equals(city1, that.city2) && Objects.equals(city2, that.city1));
    }

    @Override
    public int hashCode() {
        return city1.hashCode() + city2.hashCode();
    }
}
