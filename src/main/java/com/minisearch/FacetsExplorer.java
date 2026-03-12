package com.minisearch;

import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.facet.*;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;

public class FacetsExplorer {

    static DirectoryReader reader;
    static IndexSearcher   searcher;
    static TaxonomyReader  taxoReader;
    static FacetsConfig    facetsConfig;

    public static void main(String[] args) throws Exception {
        // Open both the main index and the taxonomy index
        Directory mainDir  = IndexConfig.openDiskDirectory();
        Directory taxoDir  = IndexConfig.openTaxonomyDirectory();

        reader      = DirectoryReader.open(mainDir);
        taxoReader  = new DirectoryTaxonomyReader(taxoDir);
        searcher    = new IndexSearcher(reader);
        facetsConfig = new FacetsConfig();

        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.println("║          FACETS EXPLORER - DAY 6                    ║");
        System.out.println("╚══════════════════════════════════════════════════════╝");
        System.out.printf("%nMain index: %,d documents%n", reader.numDocs());
        System.out.printf("Taxonomy:   %,d categories%n%n", taxoReader.getSize());

        section1_BasicFacetCounts();
        section2_FacetsWithQuery();
        section3_FacetsWithFilter();
        section4_MultipleFacetFields();
        section5_FacetDrillDown();

        reader.close();
        taxoReader.close();
        mainDir.close();
        taxoDir.close();
    }

    // ---------------------------------------------------------------
    // SECTION 1: Basic Facet Counts
    // Count all documents by category across the entire index.
    // No query filter — counts everything.
    // ---------------------------------------------------------------
    static void section1_BasicFacetCounts() throws Exception {
        printHeader("SECTION 1: Basic Facet Counts — all documents by category");

        // FacetsCollector accumulates facet data during the search
        // It runs alongside the normal TopDocs collector
        FacetsCollector fc = new FacetsCollector();

        // MatchAllDocsQuery = no filter, count everything
        // FacetsCollector.search() is a convenience method that runs
        // the query AND collects facet data in one pass
        FacetsCollector.search(searcher, new MatchAllDocsQuery(), 10, fc);

        // FastTaxonomyFacetCounts uses the taxonomy index to map
        // ordinals back to human-readable category names
        Facets facets = new FastTaxonomyFacetCounts(taxoReader, facetsConfig, fc);

        // getTopChildren(N, field) returns top N values for this field
        FacetResult result = facets.getTopChildren(10, "category_facet");

        System.out.println("Category distribution across all 60,000 documents:\\n");
        System.out.printf("  %-20s  %8s  %6s%n", "Category", "Count", "Pct");
        System.out.println("  " + "─".repeat(40));

        int total = 0;
        for (LabelAndValue lv : result.labelValues) {
            total += lv.value.intValue();
        }
        for (LabelAndValue lv : result.labelValues) {
            float pct = lv.value.floatValue() / total * 100;
            System.out.printf("  %-20s  %8d  %5.1f%%%n",
                    lv.label, lv.value.intValue(), pct);
        }
        System.out.printf("%n  Total: %,d documents%n%n", total);
    }

    // ---------------------------------------------------------------
    // SECTION 2: Facets Alongside a Query
    // This is the standard e-commerce pattern:
    // "show me results for 'search engine', and also tell me
    //  how many results are in each category"
    // Both the TopDocs and the facet counts come from the same pass.
    // ---------------------------------------------------------------
    static void section2_FacetsWithQuery() throws Exception {
        printHeader("SECTION 2: Facets Alongside a Search Query");

        Query query = new TermQuery(new Term("description", "search"));

        // Run query AND collect facets in one searcher pass
        // The MultiCollector combines two collectors:
        // - TopScoreDocCollector for ranked results
        // - FacetsCollector for category counts
        FacetsCollector      fc         = new FacetsCollector();

        // Simpler approach: use FacetsCollector.search for top docs
        // then separately get facets — two passes but cleaner code
        TopDocs results = FacetsCollector.search(searcher, query, 5, fc);

        // Print search results
        System.out.println("Query: description:\"search\"  —  top 5 results:\\n");
        for (ScoreDoc sd : results.scoreDocs) {
            Document doc = searcher.doc(sd.doc);
            System.out.printf("  [%.3f] %-12s  %s%n",
                    sd.score, doc.get("category"), doc.get("title"));
        }

        // Print facet counts for the same query
        Facets facets = new FastTaxonomyFacetCounts(taxoReader, facetsConfig, fc);
        FacetResult facetResult = facets.getTopChildren(10, "category_facet");

        System.out.println("\\nCategory breakdown for this query:\\n");
        System.out.printf("  %-20s  %8s%n", "Category", "Matches");
        System.out.println("  " + "─".repeat(32));
        for (LabelAndValue lv : facetResult.labelValues) {
            System.out.printf("  %-20s  %8d%n", lv.label, lv.value.intValue());
        }
        System.out.printf("%n  Total matching: %,d%n%n", results.totalHits.value);
    }

    // ---------------------------------------------------------------
    // SECTION 3: Facets with Filter
    // User has clicked a category filter.
    // Show results filtered to TECHNOLOGY, but show counts
    // for ALL categories (so user can switch category).
    // This is the "sticky facets" pattern used by Amazon, eBay etc.
    // ---------------------------------------------------------------
    static void section3_FacetsWithFilter() throws Exception {
        printHeader("SECTION 3: Sticky Facets — filtered results + all-category counts");

        Query baseQuery   = new TermQuery(new Term("description", "search"));
        Query catFilter   = new TermQuery(new Term("category", "TECH"));

        // Filtered query: only TECHNOLOGY results
        Query filteredQuery = new BooleanQuery.Builder()
                .add(baseQuery,  BooleanClause.Occur.MUST)
                .add(catFilter,  BooleanClause.Occur.FILTER)
                .build();

        // Run filtered query for actual results
        FacetsCollector filteredFc = new FacetsCollector();
        TopDocs filteredResults    = FacetsCollector.search(
                searcher, filteredQuery, 5, filteredFc);

        System.out.println("Results filtered to TECHNOLOGY:\\n");
        for (ScoreDoc sd : filteredResults.scoreDocs) {
            Document doc = searcher.doc(sd.doc);
            System.out.printf("  [%.3f] %s%n", sd.score, doc.get("title"));
        }

        // Run UNFILTERED base query separately just for facet counts
        // This gives "how many in each category if filter were removed"
        FacetsCollector allCatFc = new FacetsCollector();
        FacetsCollector.search(searcher, baseQuery, 1, allCatFc);

        Facets allFacets = new FastTaxonomyFacetCounts(taxoReader, facetsConfig, allCatFc);
        FacetResult allCounts = allFacets.getTopChildren(10, "category_facet");

        System.out.println("\\nAll category counts (unfiltered — for sidebar display):\\n");
        System.out.printf("  %-20s  %8s  %s%n", "Category", "Count", "");
        System.out.println("  " + "─".repeat(38));
        for (LabelAndValue lv : allCounts.labelValues) {
            String marker = lv.label.equals("TECH") ? " ← active filter" : "";
            System.out.printf("  %-20s  %8d  %s%n",
                    lv.label, lv.value.intValue(), marker);
        }
        System.out.println();
        System.out.println("  This is exactly what Amazon's left-hand filter panel does.");
        System.out.println("  Active filter shown with checkmark, counts still visible.\\n");
    }

    // ---------------------------------------------------------------
    // SECTION 4: Multiple Facet Fields Simultaneously
    // Get facet counts for both category AND author in one search pass.
    // ---------------------------------------------------------------
    static void section4_MultipleFacetFields() throws Exception {
        printHeader("SECTION 4: Multiple Facet Fields in One Pass");

        Query query = new TermQuery(new Term("description", "index"));
        FacetsCollector fc = new FacetsCollector();
        TopDocs results    = FacetsCollector.search(searcher, query, 5, fc);

        Facets facets = new FastTaxonomyFacetCounts(
                taxoReader, facetsConfig, fc);

        // Category facets
        FacetResult catResult    = facets.getTopChildren(5,  "category_facet");
        // Author facets — top 5 authors who wrote about "index"
        FacetResult authorResult = facets.getTopChildren(5,  "author_facet");

        System.out.printf("Query: description:\"index\"  —  %,d total hits%n%n",
                results.totalHits.value);

        System.out.println("By Category:");
        System.out.printf("  %-20s  %8s%n", "Category", "Count");
        System.out.println("  " + "─".repeat(32));
        for (LabelAndValue lv : catResult.labelValues) {
            System.out.printf("  %-20s  %8d%n", lv.label, lv.value.intValue());
        }

        System.out.println("\\nBy Author (top 5):");
        System.out.printf("  %-20s  %8s%n", "Author", "Articles");
        System.out.println("  " + "─".repeat(32));
        for (LabelAndValue lv : authorResult.labelValues) {
            System.out.printf("  %-20s  %8d%n", lv.label, lv.value.intValue());
        }
        System.out.println();
        System.out.println("  Both facet counts came from a single searcher pass.");
        System.out.println("  Adding more facet fields has near-zero extra cost.\\n");
    }

    // ---------------------------------------------------------------
    // SECTION 5: Facet Drill-Down
    // User clicks "TECHNOLOGY" in the sidebar.
    // Show only TECHNOLOGY results, but compute facet counts
    // for a second dimension (author) within that selection.
    // This is the "drill-down then drill-sideways" pattern.
    // ---------------------------------------------------------------
    static void section5_FacetDrillDown() throws Exception {
        printHeader("SECTION 5: Drill-Down — filter by category, facet by author");

        // DrillDownQuery adds facet dimension constraints to a base query
        DrillDownQuery drillDown = new DrillDownQuery(facetsConfig,
                new TermQuery(new Term("description", "search")));

        // Drill down into TECHNOLOGY category
        drillDown.add("category_facet", "TECH");

        FacetsCollector fc  = new FacetsCollector();
        TopDocs results     = FacetsCollector.search(searcher, drillDown, 5, fc);

        System.out.println("Drill-down: description:search within TECHNOLOGY\\n");
        System.out.printf("  %,d matching documents%n%n", results.totalHits.value);

        // Show top results
        for (ScoreDoc sd : results.scoreDocs) {
            Document doc = searcher.doc(sd.doc);
            System.out.printf("  [%.3f] %-15s  %s%n",
                    sd.score, doc.get("author"), doc.get("title"));
        }

        // Facet by author within the drill-down results
        Facets facets = new FastTaxonomyFacetCounts(
                taxoReader, facetsConfig, fc);
        FacetResult authorCounts = facets.getTopChildren(8, "author_facet");

        System.out.println("\\nTop authors writing about 'search' in TECHNOLOGY:\\n");
        System.out.printf("  %-20s  %8s%n", "Author", "Articles");
        System.out.println("  " + "─".repeat(32));
        for (LabelAndValue lv : authorCounts.labelValues) {
            System.out.printf("  %-20s  %8d%n", lv.label, lv.value.intValue());
        }
        System.out.println();
        System.out.println("  This is the foundation of Elasticsearch's aggregations API.");
        System.out.println("  terms agg → facet counts by field");
        System.out.println("  filter agg → drill-down constraint\\n");
    }

    static void printHeader(String title) {
        System.out.println("\\n━━━ " + title + " ━━━\\n");
    }
}
