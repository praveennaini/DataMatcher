package com.real.matcher.model;

public class ActorAndDirector {
		private final String name;
		private final String role;

		public ActorAndDirector(String name, String role) {
			this.name = name;
			this.role = role;
		}

		public String getName() {
			return name;
		}

		public String getRole() {
			return role;
		}

		@Override
		public String toString() {
			return "ActorAndDirector [name=" + name + ", role=" + role + "]";
		}
		
	}
