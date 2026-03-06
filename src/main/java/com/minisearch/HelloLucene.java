package com.minisearch;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import java.io.IOException;

public class HelloLucene {

    // --- Sample data ---
    // Deliberately chosen so some docs share terms — you'll see scoring differences
    static final String[][] DOCS = {
        {"1", "Lucene in Action",         "Lucene is a powerful full-text search library written in Java."},
        {"2", "Introduction to Search",   "Search engines index documents and retrieve them by relevance."},
        {"3", "Java Programming",         "Java is a popular object-oriented programming language."},
        {"4", "Information Retrieval",    "Information retrieval systems return documents relevant to a query."},
        {"5", "Search Engine Internals",  "A search engine uses an inverted index to map terms to documents."},
        {"6", "Lucene Indexing",          "Lucene IndexWriter adds documents to the index segment by segment."},
        {"7", "Lucene Queries",           "Lucene supports TermQuery, BooleanQuery, PhraseQuery and more."},
        {"8", "Relevance Ranking",        "Relevance ranking scores documents using BM25 by default in Lucene."},
        {"9", "Text Analysis",            "Analyzers tokenize and normalize text before indexing or searching."},
        {"10","Full Text Search",         "Full-text search finds documents containing terms from a query string."},
        {"11","Running Systems",          "The indexer is running and indexes are being built continuously."},
        {"12","Index Operations",         "Re-indexing and indexed data must be managed by the indexer carefully."},
    };

    public static void main(String[] args) throws Exception {

        // ---------------------------------------------------------------
        // STEP 1: Create an in-memory Directory
        // ByteBuffersDirectory lives entirely in RAM — nothing written to disk.
        // Good for experimentation; you'll swap this for FSDirectory on Day 3.
        // ---------------------------------------------------------------
        Directory directory = IndexConfig.openDiskDirectory();

        // ---------------------------------------------------------------
        // STEP 2: Create an Analyzer
        // StandardAnalyzer lowercases, removes common English stop words,
        // and splits on whitespace/punctuation.
        // ---------------------------------------------------------------
        Analyzer analyzer = new EnglishAnalyzer(); //new StandardAnalyzer();

        // ---------------------------------------------------------------
        // STEP 3: Configure and open an IndexWriter
        // IndexWriterConfig binds the analyzer to the writer.
        // CREATE mode wipes any existing index — safe for our in-memory case.
        // ---------------------------------------------------------------
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        IndexWriter writer = new IndexWriter(directory, config);

        // ---------------------------------------------------------------
        // STEP 4: Build and add Documents
        // Each Document is a collection of Fields.
        // StringField — stored as-is, NOT analyzed. Good for IDs, exact values.
        // TextField   — analyzed through the analyzer. Good for human-readable text.
        //
        // Field.Store.YES means the original value is stored and can be retrieved.
        // Field.Store.NO means it's only indexed (searchable) but not retrievable.
        // ---------------------------------------------------------------
        System.out.println("=== Indexing documents ===");
        for (String[] data : DOCS) {
            Document doc = new Document();
            doc.add(new StringField("id",    data[0], Field.Store.YES));
            doc.add(new TextField("title",   data[1], Field.Store.YES));
            doc.add(new TextField("body",    data[2], Field.Store.YES));
            writer.addDocument(doc);
            System.out.printf("  Indexed [%s] %s%n", data[0], data[1]);
        }

        // Commit flushes all buffered documents to the index.
        // Without this, nothing is visible to readers.
        writer.commit();
        writer.close();
        System.out.printf("%nIndexed %d documents.%n%n", DOCS.length);

        TokenStream tokenStream = analyzer.tokenStream("body", "Lucene is a powerful full-text search library written in Java.");
        CharTermAttribute charTermAttr = tokenStream.addAttribute(CharTermAttribute.class);
        tokenStream.reset();
        System.out.println("=== Tokens from 'body' field of doc 1 ===");
        while (tokenStream.incrementToken()) {
            String token = charTermAttr.toString();
            System.out.println(" [" + token + "] (length: " + token.length() + ")");
        }
        tokenStream.end();
        tokenStream.close();

        // ---------------------------------------------------------------
        // STEP 5: Open a reader and searcher
        // DirectoryReader opens the index for reading.
        // IndexSearcher wraps it and provides the search API.
        // ---------------------------------------------------------------
        DirectoryReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        // ---------------------------------------------------------------
        // STEP 6: Run some searches and observe results
        // ---------------------------------------------------------------

        // Search 1: TermQuery on the "body" field for the term "lucene"
        // TermQuery matches documents containing that exact term (post-analysis).
        // Because StandardAnalyzer lowercases, "lucene" matches "Lucene" in the text.
        //runSearch(searcher, "body", "lucene");

        // Search 2: try "search" — appears in multiple documents, notice score differences
        //runSearch(searcher, "body", "search");

        // Search 3: try "java" — should match fewer docs
        //runSearch(searcher, "body", "java");

        // Search 4: search the "title" field instead of body
        //runSearch(searcher, "title", "lucene");

        runSearch(searcher, "body", "index");

        //analyzeText(analyzer, "body", "Java");
        //analyzeText(analyzer, "body", "Lucene is a powerful full-text search library written in Java.");
        //analyzeText(analyzer, "title", "lucene");
        //analyzeText(analyzer, "title", "\"powerful search\"");

        // ---------------------------------------------------------------
        // STEP 7: Experiment prompts (do these manually after first run)
        // ---------------------------------------------------------------
        System.out.println("=== Experiments to try ===");
        System.out.println("1. Change TermQuery term to 'Java' (capital J). Does it still match?");
        System.out.println("   Hint: StandardAnalyzer lowercases, so the term in the index is 'java'.");
        System.out.println("   A TermQuery does NOT analyze its input — case matters here.");
        System.out.println("2. Add 3 more documents about topics you care about.");
        System.out.println("3. Try Field.Store.NO on 'body' — what breaks in the output?");
        System.out.println("4. Comment out writer.commit() — what happens when you search?");

        reader.close();
        directory.close();
    }

    static void analyzeText(Analyzer analyzer, String fieldName, String text) throws ParseException {
        QueryParser parser = new QueryParser("body", analyzer);

        Query query = parser.parse(text);
        System.out.println("=== Parsed Query ===");
        System.out.println("Parsed query: " + query);
    }

    static void runSearch(IndexSearcher searcher, String field, String termText) throws Exception {
        Query query = new TermQuery(new Term(field, termText));
        TopDocs results = searcher.search(query, 5); // top 5 hits

        System.out.printf("--- Query: %s:\"%s\"  (total hits: %d) ---%n",
                field, termText, results.totalHits.value);

        if (results.scoreDocs.length == 0) {
            System.out.println("  No results found.");
        }

        for (ScoreDoc scoreDoc : results.scoreDocs) {
            // scoreDoc.doc is Lucene's internal document ID (not your "id" field)
            Document doc = searcher.doc(scoreDoc.doc);
            System.out.printf("  [score=%.4f] id=%-3s title=\"%s\"%n",
                    scoreDoc.score,
                    doc.get("id"),
                    doc.get("title"));
        }
        System.out.println();
    }
}