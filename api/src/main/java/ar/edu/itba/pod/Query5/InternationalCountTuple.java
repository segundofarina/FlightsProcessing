package ar.edu.itba.pod.Query5;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class InternationalCountTuple implements DataSerializable {
    private int accum;
    private int total;

    public InternationalCountTuple() {
    }

    public InternationalCountTuple(int accum, int total) {
        this.accum = accum;
        this.total = total;
    }

    public int getAccum() {
        return accum;
    }

    public int getTotal() {
        return total;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(accum);
        out.writeInt(total);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        accum = in.readInt();
        total = in.readInt();
    }
}
