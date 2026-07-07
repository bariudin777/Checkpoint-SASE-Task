package com.checkpoint.iot.io;

import com.checkpoint.iot.model.Reading;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses a JSON-lines file of sampled readings into {@link Reading} objects.
 *
 * <p>Each non-blank line is one JSON object, e.g.
 * <pre>{"deviceId": "455weg75uew", "temperatureC": 95.4, "timestampMillis": 45587456}</pre>
 *
 * <p>Blank lines and lines starting with {@code #} are ignored so the sample file
 * can carry comments.
 */
public class ReadingFileParser {

    private final ObjectMapper mapper = new ObjectMapper();

    /** Parses readings from a file on disk. */
    public List<Reading> parse(Path path) {
        try (InputStream in = Files.newInputStream(path)) {
            return parse(in);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read readings from " + path, e);
        }
    }

    /**
     * Parses readings from an already-open stream (e.g. a classpath resource).
     * The caller owns the stream's lifecycle.
     */
    public List<Reading> parse(InputStream in) {
        List<Reading> readings = new ArrayList<>();
        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            int lineNumber = 0;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                    continue;
                }
                try {
                    readings.add(mapper.readValue(trimmed, Reading.class));
                } catch (IOException e) {
                    throw new UncheckedIOException(
                            "Malformed reading on line " + lineNumber + ": " + trimmed, e);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read readings stream", e);
        }
        return readings;
    }
}
