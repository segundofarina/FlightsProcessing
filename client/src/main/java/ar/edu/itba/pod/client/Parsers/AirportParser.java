package ar.edu.itba.pod.client.Parsers;

import ar.edu.itba.pod.Airport;
import com.hazelcast.core.IList;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class AirportParser implements CsvParser {
    private final IList<Airport> airportsHz;
    private final List<Airport> localAirports;

    public AirportParser(IList<Airport> airportsHz) {
        this.airportsHz = airportsHz;
        this.localAirports = new ArrayList<>();
    }

    @Override
    public void loadFile(Path path) {
        try(Stream<String> stream = Files.lines(path)) {
            stream.skip(1).forEach(this::getAirportFrom);
        } catch (IOException e) {
            System.out.println("Unable to load airports");
        }
    }

    private void getAirportFrom(String line) {
        String[] column = line.split(";");
        localAirports.add(new Airport(optionalFromStr(column[1]), optionalFromStr(column[2]), removeQuotes(column[4]), removeQuotes(column[21])));

        if(localAirports.size() > 100) {
            airportsHz.addAll(localAirports);
            localAirports.clear();
        }
    }

    private String optionalFromStr(String s) {
        if(s.equals("")) {
            return null;
        }
        return s;
    }

    private String removeQuotes(String s) {
        return s.replaceAll("^\"|\"$", "");
    }


}
