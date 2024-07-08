package com.real.matcher.model;

import java.util.List;

public class ExternalMovieFeedData {
	private final String mediaId;
	private final String title;
	private final int year;
	private final List<String> actors;
	private final String director;

	public ExternalMovieFeedData(String mediaId, String title, int year, List<String> actors, String director) {
		this.mediaId = mediaId;
		this.title = title;
		this.year = year;
		this.actors = actors;
		this.director = director;
	}

	public String getMediaId() {
		return mediaId;
	}

	public String getTitle() {
		return title;
	}

	public int getYear() {
		return year;
	}

	public List<String> getActors() {
		return actors;
	}

	public String getDirector() {
		return director;
	}

	@Override
	public String toString() {
		return "ExternalMovieFeedData [mediaId=" + mediaId + ", title=" + title + ", year=" + year + ", actors="
				+ actors + ", director=" + director + "]";
	}
	
}
