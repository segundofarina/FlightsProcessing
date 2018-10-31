package ar.edu.itba.pod.client;

import ar.edu.itba.pod.FlightType;
import ar.edu.itba.pod.Movement;
import ar.edu.itba.pod.MovementType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MovementParser implements CsvParser<Movement> {
    @Override
    public List<Movement> loadFile(Path path) {
        List<String> lines = null;

        try {
             lines = Files.readAllLines(path, StandardCharsets.ISO_8859_1);
        } catch (IOException e) {
            System.out.println("unable to load file");
            e.printStackTrace();
        }

        return getMovementsFrom(lines);
    }

    private List<Movement> getMovementsFrom(List<String> lines) {
        List<Movement> movements = new ArrayList<>();

        if(lines == null) {
            return movements;
        }

        /* Avoid first line of headers */
        lines.remove(0);

        System.out.println("ACA");
        System.out.println(lines.get(0));

        for(String line : lines) {
            movements.add(getMovementFrom(line));
        }

        return movements;
    }

    private Movement getMovementFrom(String line) {
        String[] column = line.split(";");
        return new Movement(getFlightType(column[3]), getMovementType(column[4]), column[5], column[6]);
    }

    /*private Optional<FlightType> getFlightType(String s) {
        if(s.equalsIgnoreCase("cabotaje")) {
            return Optional.of(FlightType.LOCAL);
        }
        if(s.equalsIgnoreCase("internacional")) {
            return Optional.of(FlightType.INTERNATIONAL);
        }
        return Optional.empty();
    }*/

    private FlightType getFlightType(String s) {
        if(s.equalsIgnoreCase("cabotaje")) {
            return FlightType.LOCAL;
        }
        if(s.equalsIgnoreCase("internacional")) {
            return FlightType.INTERNATIONAL;
        }
        return null;
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
