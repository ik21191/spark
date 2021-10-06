package com.mps.c5.stats;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class UserHit implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private static Set<Integer> invastigationSet;
	private static Set<Integer> requestSet;
	private static Set<Integer> searchesPlatform;
	
	static {
		invastigationSet = new HashSet<>();
		invastigationSet.add(7);
		invastigationSet.add(9);
		
		requestSet = new HashSet<>();
		requestSet.add(8);
		requestSet.add(10);
		
		searchesPlatform = new HashSet<>();
		searchesPlatform.add(6);
	}

	public static Set<Integer> getInvastigationSet() {
		return invastigationSet;
	}

	public static void setInvastigationSet(Set<Integer> invastigationSet) {
		UserHit.invastigationSet = invastigationSet;
	}

	public static Set<Integer> getRequestSet() {
		return requestSet;
	}

	public static void setRequestSet(Set<Integer> requestSet) {
		UserHit.requestSet = requestSet;
	}

	public static Set<Integer> getSearchesPlatform() {
		return searchesPlatform;
	}

	public static void setSearchesPlatform(Set<Integer> searchesPlatform) {
		UserHit.searchesPlatform = searchesPlatform;
	}
		
	
}
