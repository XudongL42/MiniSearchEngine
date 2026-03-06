package com.minisearch;

import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

public class FieldTypeExplorer {

    public static void main(String[] args) throws Exception {
        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.println("║         FIELD TYPE EXPLORER - DAY 3                 ║");
        System.out.println("╚══════════════════════════════════════════════════════╝");

        Directory dir = new ByteBuffersDirectory();
        EnglishAnalyzer analyzer = new EnglishAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(dir, config);

        // ---------------------------------------------------------------
        // Index 4 documents that cover all field type scenarios
        // ---------------------------------------------------------------

        // Doc 0: Well-formed article
        Document doc0 = new Document();
        doc0.add(new StringField("id",          "article-001",       Field.Store.YES));
        doc0.add(new TextField("title",         "Lucene Search Engine Internals", Field.Store.YES));
        doc0.add(new TextField("body",          "Lucene uses an inverted index to enable fast full-text search across documents.", Field.Store.YES));
        doc0.add(new StringField("category",    "TECHNOLOGY",        Field.Store.YES));
        doc0.add(new IntPoint("year",           2024));
        doc0.add(new StoredField("year_stored", 2024));
        doc0.add(new NumericDocValuesField("year_sort", 2024));
        writer.addDocument(doc0);

        // Doc 1: Same category, different year
        Document doc1 = new Document();
        doc1.add(new StringField("id",          "article-002",       Field.Store.YES));
        doc1.add(new TextField("title",         "Elasticsearch vs Solr Comparison", Field.Store.YES));
        doc1.add(new TextField("body",          "Both Elasticsearch and Solr are built on top of Apache Lucene core libraries.", Field.Store.YES));
        doc1.add(new StringField("category",    "TECHNOLOGY",        Field.Store.YES));
        doc1.add(new IntPoint("year",           2023));
        doc1.add(new StoredField("year_stored", 2023));
        doc1.add(new NumericDocValuesField("year_sort", 2023));
        writer.addDocument(doc1);

        // Doc 2: Different category
        Document doc2 = new Document();
        doc2.add(new StringField("id",          "article-003",       Field.Store.YES));
        doc2.add(new TextField("title",         "Marathon Running Tips for Beginners", Field.Store.YES));
        doc2.add(new TextField("body",          "Running long distances requires building endurance gradually over many weeks.", Field.Store.YES));
        doc2.add(new StringField("category",    "SPORTS",            Field.Store.YES));
        doc2.add(new IntPoint("year",           2022));
        doc2.add(new StoredField("year_stored", 2022));
        doc2.add(new NumericDocValuesField("year_sort", 2022));
        writer.addDocument(doc2);

        // Doc 3: Deliberately tricky ID to show StringField vs TextField behavior
        Document doc3 = new Document();
        doc3.add(new StringField("id",          "user@example.com",  Field.Store.YES));
        doc3.add(new TextField("title",         "Email Search Integration Guide", Field.Store.YES));
        doc3.add(new TextField("body",          "Indexing email addresses requires careful field type selection.", Field.Store.YES));
        doc3.add(new StringField("category",    "TECHNOLOGY",        Field.Store.YES));
        doc3.add(new IntPoint("year",           2021));
        doc3.add(new StoredField("year_stored", 2021));
        doc3.add(new NumericDocValuesField("year_sort", 2021));
        writer.addDocument(doc3);

        writer.commit();
        writer.close();

        DirectoryReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        // ---------------------------------------------------------------
        // Experiment 1: StringField exact match
        // StringField stores the value as-is. Lookup must be exact.
        // ---------------------------------------------------------------
        System.out.println("━━━ Experiment 1: StringField exact match ━━━\n");

        // This works — exact match
        showResults(searcher, new TermQuery(new Term("id", "article-001")),
                "id:\"article-001\" (exact)", reader);

        // This fails — StringField is case-sensitive, no analysis
        showResults(searcher, new TermQuery(new Term("id", "Article-001")),
                "id:\"Article-001\" (wrong case)", reader);

        // This works — the full email is one token in a StringField
        showResults(searcher, new TermQuery(new Term("id", "user@example.com")),
                "id:\"user@example.com\" (full email)", reader);

        // This fails — StringField wasn't split on @
        showResults(searcher, new TermQuery(new Term("id", "example.com")),
                "id:\"example.com\" (partial — won't match StringField)", reader);

        // ---------------------------------------------------------------
        // Experiment 2: TextField analysis behavior
        // ---------------------------------------------------------------
        System.out.println("━━━ Experiment 2: TextField analysis behavior ━━━\n");

        // Works — EnglishAnalyzer stems "running" → "run"
        // Doc 2 body contains "Running" which stems to "run"
        showResults(searcher, new TermQuery(new Term("body", "run")),
                "body:\"run\" (stemmed form — matches 'Running' in doc)", reader);

        // Fails — "Running" is not in the index, "run" is
        showResults(searcher, new TermQuery(new Term("body", "Running")),
                "body:\"Running\" (unstemmed — won't match)", reader);

        // Works — "lucen" is the stemmed form of "Lucene"
        showResults(searcher, new TermQuery(new Term("body", "lucen")),
                "body:\"lucen\" (stemmed Lucene)", reader);

        // ---------------------------------------------------------------
        // Experiment 3: IntPoint range query
        // IntPoint is purpose-built for numeric range queries.
        // You cannot use TermQuery on an IntPoint field.
        // ---------------------------------------------------------------
        System.out.println("━━━ Experiment 3: IntPoint numeric range query ━━━\n");

        // Range: 2022 to 2024 inclusive — should match docs 0, 1, 2
        Query rangeQuery = IntPoint.newRangeQuery("year", 2022, 2024);
        showResults(searcher, rangeQuery,
                "year:[2022 TO 2024]", reader);

        // Exact: only 2023 — should match doc 1 only
        Query exactYear = IntPoint.newRangeQuery("year", 2023, 2023);
        showResults(searcher, exactYear,
                "year:[2023 TO 2023] (exact year)", reader);

        // This does NOT work — TermQuery cannot search IntPoint fields
        // It compiles fine but returns 0 results — a silent failure
        showResults(searcher, new TermQuery(new Term("year", "2024")),
                "year:\"2024\" via TermQuery (WRONG — always 0 results)", reader);
        showResults(searcher, new TermQuery(new Term("year_stored", "2024")),
                "year_stored:\"2024\" via TermQuery (WRONG — always 0 results)", reader);
        // ---------------------------------------------------------------
        // Experiment 4: BooleanQuery combining field types
        // This is how you'd combine a text search with a category filter
        // and a date range — the core pattern for filtered search.
        // ---------------------------------------------------------------
        System.out.println("━━━ Experiment 4: BooleanQuery combining field types ━━━\n");

        // Find TECHNOLOGY articles about lucene from 2023 or later
        Query textQuery    = new TermQuery(new Term("body", "lucen"));
        Query categoryFilter = new TermQuery(new Term("category", "TECHNOLOGY"));
        Query dateFilter   = IntPoint.newRangeQuery("year", 2023, 2024);

        BooleanQuery combined = new BooleanQuery.Builder()
                // MUST: document must match (affects score)
                .add(textQuery,      BooleanClause.Occur.MUST)
                // FILTER: document must match (does NOT affect score — faster)
                .add(categoryFilter, BooleanClause.Occur.FILTER)
                .add(dateFilter,     BooleanClause.Occur.FILTER)
                .build();

        showResults(searcher, combined,
                "body:lucen AND category:TECHNOLOGY AND year:[2023-2024]", reader);

        // ---------------------------------------------------------------
        // Experiment 5: Stored vs non-stored field retrieval
        // ---------------------------------------------------------------
        System.out.println("━━━ Experiment 5: Stored vs non-stored retrieval ━━━\n");

        TopDocs all = searcher.search(new MatchAllDocsQuery(), 4);
        System.out.println("For each document, what can we retrieve?\n");
        for (ScoreDoc sd : all.scoreDocs) {
            Document d = searcher.doc(sd.doc);
            System.out.printf("  DocID %d:%n", sd.doc);
            System.out.printf("    id            = %s  (StringField, stored)%n",   d.get("id"));
            System.out.printf("    title         = %s%n",                           d.get("title"));
            System.out.printf("    category      = %s  (StringField, stored)%n",   d.get("category"));
            System.out.printf("    year_stored   = %s  (StoredField, stored)%n",   d.get("year_stored"));
            System.out.printf("    year_sorted   = %s  (NumericDocValuesField, stored)%n",   d.get("year_sorted"));
            System.out.printf("    year (IntPoint) = %s  (IntPoint, NOT stored — always null)%n",
                    d.get("year"));   // will print null
            System.out.println();
        }

        reader.close();
        dir.close();
        analyzer.close();
    }

    static void showResults(IndexSearcher searcher, Query query,
                            String label, DirectoryReader reader) throws Exception {
        TopDocs results = searcher.search(query, 10);
        System.out.printf("  Query: %s%n", label);
        System.out.printf("  Hits:  %d%n", results.totalHits.value);
        for (ScoreDoc sd : results.scoreDocs) {
            Document doc = searcher.doc(sd.doc);
            System.out.printf("    [%.4f] id=%-20s title=\"%s\"%n",
                    sd.score, doc.get("id"), doc.get("title"));
        }
        System.out.println();
    }
}
