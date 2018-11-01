package ar.edu.itba.pod.client.Parsers;

import ar.edu.itba.pod.Airport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class AirportParser implements CsvParser<Airport> {

    @Override
    public List<Airport> loadFile(Path path) {
        List<String> lines = null;
        try {
            lines = Files.readAllLines(path);
        } catch (IOException e) {
            System.out.println("Unable to load airports");
        }

        return getAirportsFrom(lines);
    }

    private List<Airport> getAirportsFrom(List<String> lines) {
        List<Airport> airports = new ArrayList<>();

        if(lines == null) {
            return airports;
        }

        /* Avoid first line of headers */
        lines.remove(0);

        for(String line : lines) {
            airports.add(getAirportFrom(line));
        }

        return airports;
    }

    private Airport getAirportFrom(String line) {
        String[] column = line.split(";");
        return new Airport(optionalFromStr(column[1]), optionalFromStr(column[2]), removeQuotes(column[4]), removeQuotes(column[21]));
    }

    private Optional<String> optionalFromStr(String s) {
        if(s.equals("")) {
            return Optional.empty();
        }
        return Optional.ofNullable(s);
    }

//    private String optionalFromStr(String s) {
//        if(s.equals("")) {
//            return null;
//        }
//        return s;
//    }

    private String removeQuotes(String s) {
        return s.replaceAll("^\"|\"$", "");
    }


}
