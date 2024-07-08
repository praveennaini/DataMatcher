package com.real.matcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.real.matcher.Matcher.CsvStream;
import com.real.matcher.Matcher.DatabaseType;
import com.real.matcher.Matcher.IdMapping;
public class MatcherOtherTests {
	//feed with empty data
	  private static final Logger LOGGER = LoggerFactory.getLogger(MatcherImplTest.class);
		@Test
		public void noMatchTest() throws Exception {
			List<IdMapping> idMappings;
			try (var closer = new Closer()) {
				var moviesCsv = loadCsvFile(closer, "movies.csv");
				var actorsAndDirectorsCsv = loadCsvFile(closer, "actors_and_directors.csv");
				var matcher = new MatcherImpl(moviesCsv, actorsAndDirectorsCsv);
				var xboxCsv = loadCsvFile(closer, "no_match_xbox_feed.csv"); // feed with no matches
				idMappings = matcher.match(DatabaseType.XBOX, xboxCsv);
			}
			LOGGER.info("Total items matched: {}", idMappings.size());
			assertEquals(0, idMappings.size(), "Matches found when there should be none!");
		}

	//feed with partial data
		@Test
		public void partialMatchTest() throws Exception {
			List<IdMapping> idMappings;
			try (var closer = new Closer()) {
				var moviesCsv = loadCsvFile(closer, "movies.csv");
				var actorsAndDirectorsCsv = loadCsvFile(closer, "actors_and_directors.csv");
				var matcher = new MatcherImpl(moviesCsv, actorsAndDirectorsCsv);
				var partialFeedCsv = loadCsvFile(closer, "partial_feed.csv");
				idMappings = matcher.match(DatabaseType.XBOX, partialFeedCsv);
			}
			assertTrue(idMappings.size() > 0, "No items matched!");
			assertTrue(idMappings.size() < 5, "Too many items matched!");
		}

	//feed with invalid data
		@Test
		public void testInvalidData() throws Exception {
			try (var closer = new Closer()) {
				var moviesCsv = loadCsvFile(closer, "movies.csv");
				var actorsAndDirectorsCsv = loadCsvFile(closer, "actors_and_directors.csv");
				var matcher = new MatcherImpl(moviesCsv, actorsAndDirectorsCsv);
				var xboxCsv = loadCsvFile(closer, "missing_data_xbox_feed.csv");
				List<IdMapping> idMappings = matcher.match(DatabaseType.XBOX, xboxCsv);
				assertEquals(0, idMappings.size(), "Total items matched should be 0 due to invalid data");
			}
		}
		
		private static CsvStream loadCsvFile(Closer closer, String fileName) throws IOException {
		    LOGGER.info("reading {}", fileName);
		    var stream = MatcherImpl.class.getClassLoader().getResourceAsStream(fileName);
		    var reader = closer.register(fileName, new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8)));
		    var header = reader.readLine().trim();
		    assertFalse(header.isBlank());
		    LOGGER.info("headers: {}", header);
		    var lines = reader
		        .lines()
		        .map(String::trim)
		        .filter(x -> !x.isBlank());
		    return new CsvStream(header, lines);
		  }

		  private static class Closer implements Closeable {

		    private final List<Map.Entry<String, Closeable>> closeables = new ArrayList<>();

		    @Override
		    public void close() throws IOException {
		      closeables.forEach(e -> {
		        var name = e.getKey();
		        LOGGER.info("closing {}", name);
		        try {
		          e.getValue().close();
		          LOGGER.info("close {}", name);
		        } catch (IOException ex) {
		          LOGGER.error("can't close {}", name, ex);
		        }
		      });
		      closeables.clear();
		    }

		    public <T extends Closeable> T register(String name, T closeable) {
		      closeables.add(Map.entry(name, closeable));
		      return closeable;
		    }
		  }
}
