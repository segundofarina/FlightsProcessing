package ar.edu.itba.pod.client.Parsers;

import java.nio.file.Path;
import java.util.List;

public interface CsvParser<T> {
    List<T> loadFile(Path path);
}
