package ar.edu.itba.pod.Query3;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Objects;

public class OaciTuple implements DataSerializable {
    private String originOaci;
    private String destinationOaci;

    public OaciTuple(String originOaci, String destiantionOaci) {
        this.originOaci = originOaci;
        this.destinationOaci = destiantionOaci;
    }

    public String getOriginOaci() {
        return originOaci;
    }

    public String getDestinationOaci() {
        return destinationOaci;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OaciTuple oaciTuple = (OaciTuple) o;
        return Objects.equals(originOaci, oaciTuple.originOaci) &&
                Objects.equals(destinationOaci, oaciTuple.destinationOaci);
    }

    @Override
    public int hashCode() {
        return Objects.hash(originOaci, destinationOaci);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(originOaci);
        out.writeUTF(destinationOaci);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        originOaci      = in.readUTF();
        destinationOaci = in.readUTF();
    }
}
