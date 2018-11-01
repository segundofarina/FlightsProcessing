package ar.edu.itba.pod.Query3;

import java.io.Serializable;
import java.util.Objects;

public class OaciTuple implements Serializable {
    private final String originOaci;
    private final String destinationOaci;

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
}
