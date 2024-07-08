package com.real.matcher.model;

public class Movies {
		private final int id;
		private final String title;
		private final int year;

		public Movies(int id, String title, int year) {
			this.id = id;
			this.title = title;
			this.year = year;
		}

		public int getId() {
			return id;
		}

		public String getTitle() {
			return title;
		}

		public int getYear() {
			return year;
		}

		@Override
		public String toString() {
			return "Movie [id=" + id + ", title=" + title + ", year=" + year + "]";
		}

	}

