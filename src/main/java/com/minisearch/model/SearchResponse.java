package com.minisearch.model;

import java.util.List;
import java.util.Map;

public class SearchResponse {

    public long               total;
    public List<Hit>          hits;
    public Map<String, List<FacetBucket>> facets;
    public long               took_ms;
    public String             query_parsed; // shows what Lucene parsed from input

    public static class Hit {
        public String id;
        public String title;
        public String description;
        public String category;
        public String author;
        public String date;
        public float  score;
        public String highlight;
        public String explanation; // only populated if explain=true
    }

    public static class FacetBucket {
        public String label;
        public int    count;

        public FacetBucket(String label, int count) {
            this.label = label;
            this.count = count;
        }
    }
}
