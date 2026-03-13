package com.minisearch.api;

import com.minisearch.model.SearchResponse;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.search.*;
import org.apache.lucene.search.highlight.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResultBuilder {

    private final Analyzer analyzer;

    public ResultBuilder(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    // ---------------------------------------------------------------
    // Converts TopDocs + facets into a SearchResponse.
    // explain=true adds the full BM25 score breakdown per hit.
    // ---------------------------------------------------------------
    public SearchResponse build(
            TopDocs topDocs,
            Query query,
            IndexSearcher searcher,
            List<FacetResult> facetResults,
            String parsedQuery,
            long tookMs,
            boolean explain) throws Exception {

        SearchResponse response  = new SearchResponse();
        response.total           = topDocs.totalHits.value;
        response.took_ms         = tookMs;
        response.query_parsed    = parsedQuery;
        response.hits            = buildHits(topDocs, query, searcher, explain);
        response.facets          = buildFacets(facetResults);
        return response;
    }

    // ---------------------------------------------------------------
    // Build hit list with highlighting and optional score explanation
    // ---------------------------------------------------------------
    private List<SearchResponse.Hit> buildHits(
            TopDocs topDocs,
            Query query,
            IndexSearcher searcher,
            boolean explain) throws Exception {

        List<SearchResponse.Hit> hits = new ArrayList<>();

        // Set up highlighter — wraps matching terms in <em> tags
        QueryScorer  scorer      = new QueryScorer(query);
        Highlighter  highlighter = new Highlighter(
                new SimpleHTMLFormatter("<em>", "</em>"), scorer);
        highlighter.setTextFragmenter(new SimpleFragmenter(120));

        for (ScoreDoc sd : topDocs.scoreDocs) {
            Document doc = searcher.doc(sd.doc);
            SearchResponse.Hit hit = new SearchResponse.Hit();

            hit.id          = doc.get("id");
            hit.title       = doc.get("title");
            hit.description = doc.get("description");
            hit.category    = doc.get("category");
            hit.author      = doc.get("author");
            hit.date        = doc.get("date");
            hit.score       = sd.score;
            hit.highlight   = buildHighlight(
                    highlighter, doc.get("description"));

            // Score explanation: only computed if explicitly requested
            // Explanation.toString() is expensive — don't compute for every request
            if (explain) {
                Explanation exp = searcher.explain(query, sd.doc);
                hit.explanation = exp.toString();
            }

            hits.add(hit);
        }
        return hits;
    }

    private String buildHighlight(Highlighter highlighter, String text) {
        if (text == null || text.isBlank()) return null;
        try {
            org.apache.lucene.analysis.TokenStream ts =
                    analyzer.tokenStream("description", text);
            String fragment = highlighter.getBestFragment(ts, text);
            return fragment != null ? fragment : truncate(text, 120);
        } catch (Exception e) {
            return truncate(text, 120);
        }
    }

    // ---------------------------------------------------------------
    // Convert FacetResult list into the response map structure
    // ---------------------------------------------------------------
    private Map<String, List<SearchResponse.FacetBucket>> buildFacets(
            List<FacetResult> facetResults) {

        Map<String, List<SearchResponse.FacetBucket>> facets = new HashMap<>();
        if (facetResults == null) return facets;

        for (FacetResult fr : facetResults) {
            if (fr == null) continue;
            List<SearchResponse.FacetBucket> buckets = new ArrayList<>();
            for (LabelAndValue lv : fr.labelValues) {
                buckets.add(new SearchResponse.FacetBucket(
                        lv.label, lv.value.intValue()));
            }
            // Use the field name as the key, stripping "_facet" suffix
            // so the API returns "category" not "category_facet"
            String key = fr.dim.replace("_facet", "");
            facets.put(key, buckets);
        }
        return facets;
    }

    private String truncate(String s, int max) {
        if (s == null) return null;
        return s.length() > max ? s.substring(0, max - 3) + "..." : s;
    }
}
