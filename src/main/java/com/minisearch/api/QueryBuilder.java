package com.minisearch.api;

import com.minisearch.model.SearchRequest;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;

import java.util.HashMap;
import java.util.Map;

public class QueryBuilder {

    private final Analyzer analyzer;

    // Fields searched by default, with their boost weights.
    // Title matches are worth 3x body matches — same decision as Day 5.
    private static final String[] SEARCH_FIELDS  = {"title", "description"};
    private static final float[]  FIELD_BOOSTS   = {3.0f,    1.0f};

    public QueryBuilder(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    // ---------------------------------------------------------------
    // Build the full query from a SearchRequest.
    // Returns both the query and its string representation for logging.
    // ---------------------------------------------------------------
    public BuildResult build(SearchRequest req) throws ParseException {

        // Step 1: build the text query from q= parameter
        Query textQuery = buildTextQuery(req.query);

        // Step 2: wrap with filters if provided
        Query fullQuery = applyFilters(textQuery, req);

        return new BuildResult(fullQuery, textQuery.toString());
    }

    // ---------------------------------------------------------------
    // Text query: uses MultiFieldQueryParser so a single q= string
    // searches both title and description simultaneously.
    // "*" becomes MatchAllDocsQuery — returns everything.
    // ---------------------------------------------------------------
    private Query buildTextQuery(String queryString) throws ParseException {
        if (queryString == null || queryString.equals("*") || queryString.isBlank()) {
            return new MatchAllDocsQuery();
        }

        Map<String, Float> boostMap = new HashMap<>();
        for (int i = 0; i < SEARCH_FIELDS.length; i++) {
            boostMap.put(SEARCH_FIELDS[i], FIELD_BOOSTS[i]);
        }

        MultiFieldQueryParser parser = new MultiFieldQueryParser(
                SEARCH_FIELDS, analyzer, boostMap);

        // Default operator AND: all terms must appear
        // More precise than OR — fewer but more relevant results
        parser.setDefaultOperator(QueryParser.Operator.AND);

        try {
            return parser.parse(QueryParser.escape(queryString));
        } catch (ParseException e) {
            // If query parse fails (e.g. unbalanced quotes),
            // fall back to a simple single-field search
            return new TermQuery(new Term("title",
                    queryString.toLowerCase().trim()));
        }
    }

    // ---------------------------------------------------------------
    // Filters: applied as FILTER clauses (no score contribution,
    // results are cached by LRUQueryCache between requests).
    // ---------------------------------------------------------------
    private Query applyFilters(Query textQuery, SearchRequest req) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(textQuery, BooleanClause.Occur.MUST);

        // Category filter
        if (req.category != null && !req.category.isBlank()) {
            builder.add(
                new TermQuery(new Term("category", req.category.toUpperCase())),
                BooleanClause.Occur.FILTER);
        }

        // Date range filter — YYYYMMDD integer format
        if (req.dateFrom != null || req.dateTo != null) {
            int from = parseDate(req.dateFrom, 0);
            int to   = parseDate(req.dateTo,   Integer.MAX_VALUE);
            builder.add(
                IntPoint.newRangeQuery("date_int", from, to),
                BooleanClause.Occur.FILTER);
        }

        BooleanQuery built = builder.build();

        // If there are no filter clauses, just return the text query directly
        // A BooleanQuery with a single MUST clause is functionally identical
        // but adds unnecessary wrapping — returning the inner query is cleaner
        if (built.clauses().size() == 1) {
            return textQuery;
        }
        return built;
    }

    private int parseDate(String dateStr, int defaultValue) {
        if (dateStr == null || dateStr.isBlank()) return defaultValue;
        try {
            // Accept YYYY-MM-DD or YYYYMMDD
            return Integer.parseInt(dateStr.replace("-", ""));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    // ---------------------------------------------------------------
    // Build a Sort object from the sort= parameter
    // ---------------------------------------------------------------
    public Sort buildSort(String sortParam) {
        if (sortParam == null) return Sort.RELEVANCE;
        return switch (sortParam.toLowerCase()) {
            case "date_asc"  -> new Sort(
                    new SortField("date_sort", SortField.Type.LONG, false));
            case "date_desc" -> new Sort(
                    new SortField("date_sort", SortField.Type.LONG, true));
            default          -> Sort.RELEVANCE;
        };
    }

    public record BuildResult(Query query, String parsedString) {}
}
