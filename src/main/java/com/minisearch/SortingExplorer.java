package com.minisearch;

import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;

public class SortingExplorer {

    static DirectoryReader reader;
    static IndexSearcher   searcher;

    public static void main(String[] args) throws Exception {
        Directory directory = IndexConfig.openDiskDirectory();
        reader   = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);

        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.println("║         SORTING EXPLORER - DAY 5                    ║");
        System.out.println("╚══════════════════════════════════════════════════════╝");
        System.out.printf("%nIndex: %,d documents%n%n", reader.numDocs());

        section1_DefaultRelevanceSort();
        section2_SortByDate();
        section3_SortByMultipleFields();
        section4_PaginationWithSort();
        section5_SortVsRelevanceTradeoff();

        reader.close();
        directory.close();
    }

    // ---------------------------------------------------------------
    // SECTION 1: Default relevance sort (baseline)
    // Results sorted by BM25 score descending.
    // This is what every search above has been doing implicitly.
    // ---------------------------------------------------------------
    static void section1_DefaultRelevanceSort() throws Exception {
        printHeader("SECTION 1: Default Relevance Sort (BM25 score descending)");

        Query query = new TermQuery(new Term("description", "search"));

        // Explicit relevance sort — identical to not passing a Sort at all
        TopDocs results = searcher.search(query, 5, Sort.RELEVANCE);

        System.out.println("Query: description:search — sorted by relevance (default)\\n");
        printResults(results, "Score", sd -> String.format("%.4f", sd.score));
    }

    // ---------------------------------------------------------------
    // SECTION 2: Sort by date (NumericDocValues)
    // Uses the date_sort NumericDocValuesField you added on Day 3.
    // IMPORTANT: you can only sort by a field that has DocValues.
    // Trying to sort by a stored field without DocValues throws an error.
    // ---------------------------------------------------------------
    static void section2_SortByDate() throws Exception {
        printHeader("SECTION 2: Sort by Date using NumericDocValuesField");

        Query query = new TermQuery(new Term("description", "search"));

        // Sort by date_sort ascending (oldest first)
        Sort sortByDateAsc = new Sort(
                new SortField("date_sort", SortField.Type.LONG, false));
        TopFieldDocs ascResults = searcher.search(query, 5, sortByDateAsc);

        System.out.println("Oldest first (date_sort ASC):\\n");
        printFieldResults(ascResults, "Date",
                sd -> getField(searcher, sd, "date"));

        // Sort by date_sort descending (newest first)
        Sort sortByDateDesc = new Sort(
                new SortField("date_sort", SortField.Type.LONG, true));
        TopFieldDocs descResults = searcher.search(query, 5, sortByDateDesc);

        System.out.println("Newest first (date_sort DESC):\\n");
        printFieldResults(descResults, "Date",
                sd -> getField(searcher, sd, "date"));

        // Key observation: same query, same documents,
        // completely different order. Score is no longer the ranking signal.
        System.out.println("  Observation: relevance score is computed but ignored for ordering.");
        System.out.println("  Documents are ranked purely by their date_sort DocValues value.\\n");
    }

    // ---------------------------------------------------------------
    // SECTION 3: Multi-field sort
    // Primary sort: date descending
    // Secondary sort (tiebreak): relevance score descending
    // This is the "newest first, then by relevance" pattern
    // common in news search and social feeds.
    // ---------------------------------------------------------------
    static void section3_SortByMultipleFields() throws Exception {
        printHeader("SECTION 3: Multi-Field Sort — date DESC, then score DESC");

        Query query = new TermQuery(new Term("category", "TECH"));

        // Primary: newest first. Secondary: higher score first.
        // SortField.FIELD_SCORE is the built-in relevance sort field.
        Sort multiSort = new Sort(
                new SortField("date_sort", SortField.Type.LONG,  true),  // date DESC
                SortField.FIELD_SCORE                                     // score DESC
        );

        TopFieldDocs results = searcher.search(query, 8, multiSort);

        System.out.println("TECHNOLOGY articles: newest first, then by relevance\\n");
        System.out.printf("  %-12s  %-8s  %s%n", "Date", "Score", "Title");
        System.out.println("  " + "─".repeat(65));

        for (ScoreDoc sd : results.scoreDocs) {
            FieldDoc fd = (FieldDoc) sd;
            Document doc = searcher.doc(fd.doc);
            // fd.fields[] contains the sort values in order
            // fd.fields[0] = date_sort value, fd.fields[1] = score
            Object dateVal  = fd.fields[0];
            System.out.printf("  %-12s  %-8.4f  %s%n",
                    doc.get("date"),
                    fd.score,
                    truncate(doc.get("title"), 45));
        }
        System.out.println();

        // Compare: same query with pure relevance sort
        TopDocs relevanceResults = searcher.search(query, 8);
        System.out.println("Same query, pure relevance sort:\\n");
        System.out.printf("  %-12s  %-8s  %s%n", "Date", "Score", "Title");
        System.out.println("  " + "─".repeat(65));
        for (ScoreDoc sd : relevanceResults.scoreDocs) {
            Document doc = searcher.doc(sd.doc);
            System.out.printf("  %-12s  %-8.4f  %s%n",
                    doc.get("date"), sd.score,
                    truncate(doc.get("title"), 45));
        }
        System.out.println();
    }

    // ---------------------------------------------------------------
    // SECTION 4: Pagination with Sort
    // searchAfter() is Lucene's efficient deep pagination.
    // It's far more efficient than skip/offset for large result sets
    // because it doesn't score/load the skipped pages.
    // This maps directly to Elasticsearch's search_after parameter.
    // ---------------------------------------------------------------
    static void section4_PaginationWithSort() throws Exception {
        printHeader("SECTION 4: Pagination with searchAfter()");

        Query query    = new TermQuery(new Term("category", "TECH"));
        Sort sort      = new Sort(new SortField("date_sort", SortField.Type.LONG, true));
        int  pageSize  = 3;

        System.out.println("Paginating TECHNOLOGY articles, 3 per page\\n");

        // Page 1
        TopFieldDocs page1 = searcher.search(query, pageSize, sort);
        System.out.println("Page 1:");
        printPage(page1);

        // Page 2 — pass the last FieldDoc from page 1 as the "after" cursor
        // searchAfter uses the sort values of the last result as a bookmark.
        // This is O(pageSize) not O(offset + pageSize) — critical for deep pages.
        if (page1.scoreDocs.length == pageSize) {
            FieldDoc lastOfPage1 = (FieldDoc) page1.scoreDocs[pageSize - 1];
            TopFieldDocs page2   = (TopFieldDocs) searcher.searchAfter(lastOfPage1, query, pageSize, sort);
            System.out.println("Page 2:");
            printPage(page2);

            // Page 3
            if (page2.scoreDocs.length == pageSize) {
                FieldDoc lastOfPage2 = (FieldDoc) page2.scoreDocs[pageSize - 1];
                TopFieldDocs page3   = (TopFieldDocs) searcher.searchAfter(lastOfPage2, query, pageSize, sort);
                System.out.println("Page 3:");
                printPage(page3);
            }
        }

        System.out.println("  searchAfter() maps to ES's search_after parameter.");
        System.out.println("  Never use from/size offset for deep pagination —");
        System.out.println("  it scores all skipped documents unnecessarily.\\n");
    }

    // ---------------------------------------------------------------
    // SECTION 5: The Relevance vs Sort Tradeoff
    // Sorting by a field completely overrides relevance ordering.
    // This section makes that tradeoff visible and concrete.
    // ---------------------------------------------------------------
    static void section5_SortVsRelevanceTradeoff() throws Exception {
        printHeader("SECTION 5: Relevance vs Sort Tradeoff");

        // A query where relevance clearly differentiates results
        Query query = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("title", "search")), BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term("description",  "search")), BooleanClause.Occur.SHOULD)
                .build();

        // Pure relevance
        TopDocs byRelevance = searcher.search(query, 5);
        System.out.println("By relevance — most relevant first:");
        System.out.printf("  %-8s  %-12s  %s%n", "Score", "Date", "Title");
        System.out.println("  " + "─".repeat(65));
        for (ScoreDoc sd : byRelevance.scoreDocs) {
            Document doc = searcher.doc(sd.doc);
            System.out.printf("  %-8.4f  %-12s  %s%n",
                    sd.score, doc.get("date"),
                    truncate(doc.get("title"), 40));
        }
        System.out.println();

        // Pure date sort — relevance ignored
        Sort dateSort   = new Sort(new SortField("date_sort", SortField.Type.LONG, true));
        TopFieldDocs byDate = searcher.search(query, 5, dateSort);
        System.out.println("By date DESC — newest first (relevance ignored):");
        System.out.printf("  %-8s  %-12s  %s%n", "Score", "Date", "Title");
        System.out.println("  " + "─".repeat(65));
        for (ScoreDoc sd : byDate.scoreDocs) {
            Document doc = searcher.doc(sd.doc);
            System.out.printf("  %-8.4f  %-12s  %s%n",
                    sd.score, doc.get("date"),
                    truncate(doc.get("title"), 40));
        }
        System.out.println();

        System.out.println("  Key insight:");
        System.out.println("  When you sort by a field, scores are still computed");
        System.out.println("  but not used for ordering. The most relevant document");
        System.out.println("  might appear on page 10 when sorting by date.");
        System.out.println("  This is why ES exposes both 'sort' and '_score' separately.\\n");
    }

    // ---------------------------------------------------------------
    // HELPERS
    // ---------------------------------------------------------------

    interface FieldExtractor {
        String extract(ScoreDoc sd) throws Exception;
    }

    static void printResults(TopDocs results, String colHeader,
                             FieldExtractor extractor) throws Exception {
        System.out.printf("  %-10s  %-10s  %s%n", colHeader, "Category", "Title");
        System.out.println("  " + "─".repeat(65));
        for (ScoreDoc sd : results.scoreDocs) {
            Document doc = searcher.doc(sd.doc);
            System.out.printf("  %-10s  %-10s  %s%n",
                    extractor.extract(sd),
                    doc.get("category"),
                    truncate(doc.get("title"), 40));
        }
        System.out.println();
    }

    static void printFieldResults(TopFieldDocs results, String colHeader,
                                  FieldExtractor extractor) throws Exception {
        System.out.printf("  %-12s  %-10s  %s%n", colHeader, "Category", "Title");
        System.out.println("  " + "─".repeat(65));
        for (ScoreDoc sd : results.scoreDocs) {
            Document doc = searcher.doc(sd.doc);
            System.out.printf("  %-12s  %-10s  %s%n",
                    extractor.extract(sd),
                    doc.get("category"),
                    truncate(doc.get("title"), 40));
        }
        System.out.println();
    }

    static void printPage(TopFieldDocs page) throws Exception {
        for (ScoreDoc sd : page.scoreDocs) {
            Document doc = searcher.doc(sd.doc);
            System.out.printf("  [%s] %s%n", doc.get("date"),
                    truncate(doc.get("title"), 50));
        }
        System.out.println();
    }

    static String getField(IndexSearcher s, ScoreDoc sd, String field) {
        try {
            return s.doc(sd.doc).get(field);
        } catch (Exception e) {
            return "?";
        }
    }

    static String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() > max ? s.substring(0, max - 3) + "..." : s;
    }

    static void printHeader(String title) {
        System.out.println("\\n━━━ " + title + " ━━━\\n");
    }
}