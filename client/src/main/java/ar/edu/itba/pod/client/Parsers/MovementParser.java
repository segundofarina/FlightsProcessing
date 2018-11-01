package ar.edu.itba.pod.client.Parsers;

import ar.edu.itba.pod.FlightType;
import ar.edu.itba.pod.Movement;
import ar.edu.itba.pod.MovementType;
import com.hazelcast.core.IList;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class MovementParser implements CsvParser {
    private final IList<Movement> movements;
    private final List<Movement> localMovements;

    public MovementParser(IList<Movement> movements) {
        this.movements = movements;
        this.localMovements = new ArrayList<>();
    }

    @Override
    public void loadFile(Path path) {
        try(Stream<String> stream = Files.lines(path, StandardCharsets.ISO_8859_1)) {
            stream.skip(1).forEach(this::getMovementFrom);
        } catch (IOException e) {
            System.out.println("Unable to load movements");
        }
    }

    private void getMovementFrom(String line) {
        String[] column = line.split(";");
        localMovements.add(new Movement(getFlightType(column[3]), getMovementType(column[4]), column[5], column[6]));

        if(localMovements.size() > 100) {
            movements.addAll(localMovements);
            localMovements.clear();
        }
    }

    private Optional<FlightType> getFlightType(String s) {
        if(s.equalsIgnoreCase("cabotaje")) {
            return Optional.of(FlightType.LOCAL);
        }
        if(s.equalsIgnoreCase("internacional")) {
            return Optional.of(FlightType.INTERNATIONAL);
        }
        return Optional.empty();
    }

    private MovementType getMovementType(String s) {
        if(s.equalsIgnoreCase("despegue")) {
            return MovementType.TAKE_OF;
        }

        if(s.equalsIgnoreCase("aterrizaje")) {
            return MovementType.LANDING;
        }

        throw new IllegalArgumentException("Illegal movement type: " + s);
    }
}
