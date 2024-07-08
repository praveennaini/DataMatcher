package com.real.matcher;

import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import com.real.matcher.model.ActorAndDirector;
import com.real.matcher.model.ExternalMovieFeedData;
import com.real.matcher.model.Movies;

public class MatcherImpl implements Matcher {

	private static final Logger LOGGER = LoggerFactory.getLogger(MatcherImpl.class);
	private Map<Integer, Movies> movieDb;
	private Map<Integer, List<ActorAndDirector>> actorAndDirectorDb;
	private Map<String, List<Movies>> movieIndex;

	public MatcherImpl(CsvStream movieDb, CsvStream actorAndDirectorDb) {
		LOGGER.info("importing database");
		this.movieDb = parseMovies(movieDb);
		this.actorAndDirectorDb = parseActorsAndDirectors(actorAndDirectorDb);
		this.movieIndex = createMovieIndexForSearch(this.movieDb);
		LOGGER.info("database imported");
	}

	@Override
	public List<IdMapping> match(DatabaseType databaseType, CsvStream externalDb) {
		List<ExternalMovieFeedData> externalMovies = parseExternalMovies(externalDb);
		List<IdMapping> idMappings = new ArrayList<>();
		Set<String> uniqueMediaIds = new HashSet<>();
		LOGGER.info("Checking the size of ExternalMovieFeedData after parsing:" + externalMovies.size());
		for (ExternalMovieFeedData externalMovieFeedData : externalMovies) {
			findBestMatch(externalMovieFeedData).ifPresent(internalId -> {
				if (uniqueMediaIds.add(externalMovieFeedData.getMediaId())) {
					idMappings.add(new IdMapping(internalId, externalMovieFeedData.getMediaId()));
				}
			});
		}
		LOGGER.info("Matched data for the given dataset is:" + idMappings.size());
		return idMappings;
	}

	// Parsing the movies data and storing the data in the HashMap
	private Map<Integer, Movies> parseMovies(CsvStream csvStream) {
		LOGGER.info("Parsing the Movies Data from the CSV stream: {}");
		Map<Integer, Movies> movieMap = new HashMap<>();
		try (CSVReader reader = new CSVReader(new StringReader(csvStream.getHeaderRow() + "\n"
				+ String.join("\n", csvStream.getDataRows().collect(Collectors.toList()))))) {
			String[] nextLine;
			reader.readNext();
			while ((nextLine = reader.readNext()) != null) {
				int id = Integer.parseInt(nextLine[0].trim());
				String title = nextLine[1].trim();
				String yearString = nextLine[2].trim();
				if (yearString.equalsIgnoreCase("NULL") || yearString.isEmpty()) { // Handled the year data when its a NULL
					LOGGER.warn("Skipping movie with invalid year: id={}, title={}", id, title);
					continue;
				}

				int year;
				try {
					year = Integer.parseInt(yearString);
				} catch (NumberFormatException e) {
					LOGGER.warn("Skipping movie with invalid year format: id={}, title={}, year={}", id, title,
							yearString);
					continue;
				}
				movieMap.put(id, new Movies(id, title, year));

			}
		} catch (IOException | CsvValidationException e) {
			LOGGER.error("Error parsing movies from CSV stream: {}", e.getMessage());
		}
		LOGGER.info("Completed parsing the Movie Data from CSV stream and the data size is: {}" + movieMap.size());
		return movieMap;
	}

	// Parsing the ActorAndDirector data and storing the data in the HashMap
	private Map<Integer, List<ActorAndDirector>> parseActorsAndDirectors(CsvStream csvStream) {
		LOGGER.info("Parsing the ActorAndDirector Data from the CSV stream: {}");
		Map<Integer, List<ActorAndDirector>> actorAndDirectorMap = new HashMap<>();
		try (CSVReader reader = new CSVReader(new StringReader(csvStream.getHeaderRow() + "\n"
				+ String.join("\n", csvStream.getDataRows().collect(Collectors.toList()))))) {
			String[] nextLine;
			reader.readNext();
			while ((nextLine = reader.readNext()) != null) {
				int movieId = Integer.parseInt(nextLine[0].trim());
				String name = nextLine[1].trim();
				String role = nextLine[2].trim();
				actorAndDirectorMap.computeIfAbsent(movieId, k -> new ArrayList<>())
						.add(new ActorAndDirector(name, role));
			}
		} catch (IOException | CsvValidationException e) {
			LOGGER.error("Error parsing ActorAndDirector from CSV stream: {}", e.getMessage());
		}
		LOGGER.info("Completed parsing the ActorAndDirector Data from CSV stream and the data size is: {}" + actorAndDirectorMap.size());
		return actorAndDirectorMap;
	}

	// Parsing the ExternalMovieFeedData data and storing the data in the List
	private List<ExternalMovieFeedData> parseExternalMovies(Matcher.CsvStream csvStream) {
		LOGGER.info("Parsing the ExternalMovieFeedData Data from the CSV stream: {}");
		List<ExternalMovieFeedData> externalMovies = new ArrayList<>();
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");

		try (CSVReader reader = new CSVReader(new StringReader(csvStream.getHeaderRow() + "\n"
				+ String.join("\n", csvStream.getDataRows().collect(Collectors.toList()))))) {
			String[] nextLine;
			reader.readNext();

			while ((nextLine = reader.readNext()) != null) {
				String mediaId = nextLine[2].trim();
				String title = nextLine[3].trim();
				String releaseDateStr = nextLine[4].trim();
				Date releaseDate = dateFormat.parse(releaseDateStr);
				Calendar calendar = Calendar.getInstance();
				calendar.setTime(releaseDate);
				int year = calendar.get(Calendar.YEAR);
				List<String> actors = Arrays.asList(nextLine[15].split(","));
				String director = nextLine[16].trim();
				externalMovies.add(new ExternalMovieFeedData(mediaId, title, year, actors, director));
			}
		} catch (IOException | CsvValidationException | ParseException e) {
			LOGGER.error("Error parsing ExternalMovieFeedData from CSV stream: {}", e.getMessage());
		}
		LOGGER.info("Completed parsing the ExternalMovieFeedData Data from CSV stream and the data size is: {}" + externalMovies.size());
		return externalMovies;
	}

	// Create a Search based mechanism to search the data efficiently.
	private Map<String, List<Movies>> createMovieIndexForSearch(Map<Integer, Movies> movieDb) {
		Map<String, List<Movies>> index = new HashMap<>();
		for (Movies movie : movieDb.values()) {
			String key = createIndexKeyFoSearch(movie.getTitle(), movie.getYear());
			index.computeIfAbsent(key, k -> new ArrayList<>()).add(movie);
		}
		return index;
	}

	private String createIndexKeyFoSearch(String title, int year) {
		return title.toLowerCase() + "_" + year;
	}

	// Iterates over all possibleMatches to find the best match by checking both the director and actor names.
	private Optional<Integer> findBestMatch(ExternalMovieFeedData externalMovieFeedData) {
		String key = createIndexKeyFoSearch(externalMovieFeedData.getTitle(), externalMovieFeedData.getYear());
		List<Movies> possibleMatches = movieIndex.getOrDefault(key, Collections.emptyList());

		if (!possibleMatches.isEmpty()) {
			for (Movies match : possibleMatches) {
				List<ActorAndDirector> internalActorsAndDirectors = actorAndDirectorDb.getOrDefault(match.getId(),
						Collections.emptyList());
				boolean directorMatches = internalActorsAndDirectors.stream()
						.anyMatch(ad -> ad.getRole().equalsIgnoreCase("director")
								&& ad.getName().equalsIgnoreCase(externalMovieFeedData.getDirector()));
				boolean actorsMatch = externalMovieFeedData.getActors().stream()
						.allMatch(externalActor -> internalActorsAndDirectors.stream()
								.anyMatch(ad -> ad.getRole().equalsIgnoreCase("cast")
										&& ad.getName().equalsIgnoreCase(externalActor.trim())));

				if (directorMatches || actorsMatch) {
					return Optional.of(match.getId());
				}
			}
		}
		return Optional.empty();
	}
}