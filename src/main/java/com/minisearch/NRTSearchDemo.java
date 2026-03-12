package com.minisearch;

import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class NRTSearchDemo {

    static IndexWriter     writer;
    static SearcherManager searcherManager;
    static EnglishAnalyzer analyzer;
    static AtomicInteger   docCounter = new AtomicInteger(100_000);

    public static void main(String[] args) throws Exception {
        Directory directory = IndexConfig.openDiskDirectory();
        analyzer = new EnglishAnalyzer();

        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        // APPEND to existing index — don't wipe the 60k documents
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        writer = new IndexWriter(directory, config);

        // ---------------------------------------------------------------
        // SearcherManager is the production-safe wrapper for NRT search.
        // It manages a pool of IndexSearcher instances, automatically
        // refreshing them when the writer has new data.
        //
        // acquire()      → get a searcher (thread-safe)
        // release()      → return it (MUST always be called, use try/finally)
        // maybeRefresh() → check writer for new docs, swap reader if changed
        // ---------------------------------------------------------------
        searcherManager = new SearcherManager(writer, new SearcherFactory());

        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.println("║         NRT SEARCH DEMO - DAY 6                     ║");
        System.out.println("╚══════════════════════════════════════════════════════╝\\n");

        section1_BasicNRT();
        section2_SearcherManagerPattern();
        section3_ConcurrentWritesAndReads();
        section4_DeleteAndUpdate();

        writer.close();
        searcherManager.close();
        analyzer.close();
        directory.close();
    }

    // ---------------------------------------------------------------
    // SECTION 1: Basic NRT — add a doc, find it without commit
    // This demonstrates the core NRT capability:
    // documents are visible after maybeRefresh(), not after commit().
    // commit() is for durability (survives crash), not visibility.
    // ---------------------------------------------------------------
    static void section1_BasicNRT() throws Exception {
        printHeader("SECTION 1: Basic NRT — visible before commit");

        String uniqueTitle = "NRT TEST UNIQUE ARTICLE " + System.currentTimeMillis();

        // Debug: see what tokens EnglishAnalyzer produces for the title
        System.out.println("DEBUG: Title to index: \"" + uniqueTitle + "\"");
        System.out.println("DEBUG: Tokens produced by EnglishAnalyzer: " + 
            AnalyzerPlayground.getTokens(analyzer, "title", uniqueTitle));
        System.out.println();

        // Confirm it doesn't exist yet
        System.out.println("Before adding document:");
        long before = countMatching("title", uniqueTitle.toLowerCase().split(" ")[0]);
        System.out.printf("  Docs matching 'nrt': %d%n%n", before);

        // Add document via IndexWriter — NOT committed to disk yet
        Document doc = buildArticle(
                String.valueOf(docCounter.incrementAndGet()),
                uniqueTitle,
                "NRT allows near real time document visibility without committing",
                "TECH");
        writer.addDocument(doc);

        // Without refresh: searcher still sees old snapshot
        System.out.println("After addDocument(), BEFORE maybeRefresh():");
        long afterAdd = countMatching("title", "nrt");
        System.out.printf("  Docs matching 'nrt': %d  (still 0 — reader not refreshed)%n%n",
                afterAdd);

        // maybeRefresh() opens a new reader from the writer's RAM buffer
        // No disk write happens here — that's what makes it "near real time"
        searcherManager.maybeRefresh();

        System.out.println("After maybeRefresh():");
        long afterRefresh = countMatching("title", "nrt");
        System.out.printf("  Docs matching 'nrt': %d  (document now visible!)%n%n",
                afterRefresh);

        System.out.println("After maybeRefresh() but BEFORE commit():");
        System.out.println("  Document IS searchable (in writer's RAM buffer)");
        System.out.println("  Document is NOT durable (lost if process crashes)");

        // Commit for durability — survives crashes
        writer.commit();
        System.out.println("After commit():");
        System.out.println("  Document IS durable (written to disk + fsync)\\n");
    }

    // ---------------------------------------------------------------
    // SECTION 2: SearcherManager Pattern
    // The correct way to use SearcherManager in production.
    // acquire/release MUST be paired — always use try/finally.
    // ---------------------------------------------------------------
    static void section2_SearcherManagerPattern() throws Exception {
        printHeader("SECTION 2: SearcherManager acquire/release pattern");

        // Add a batch of documents
        System.out.println("Adding 5 documents in a batch...");
        for (int i = 0; i < 5; i++) {
            writer.addDocument(buildArticle(
                    String.valueOf(docCounter.incrementAndGet()),
                    "Batch Document Number " + i + " About Search Systems",
                    "This document was added in a batch to demonstrate NRT behavior",
                    "TECH"));
        }

        // The correct acquire/release pattern
        // CRITICAL: always release in finally block
        // Failure to release leaks the searcher reference —
        // Lucene cannot close the underlying reader while references are held
        searcherManager.maybeRefresh();

        IndexSearcher searcherRef = searcherManager.acquire();
        try {
            Query query  = new TermQuery(new Term("title", "batch"));
            TopDocs hits = searcherRef.search(query, 10);
            System.out.printf("  Acquired searcher, found %d 'batch' docs%n",
                    hits.totalHits.value);

            // Demonstrate that this searcher is a stable snapshot
            // Even if more documents are added now, THIS searcher
            // won't see them until it's released and re-acquired
            writer.addDocument(buildArticle(
                    String.valueOf(docCounter.incrementAndGet()),
                    "Another Batch Document Added While Searcher Held",
                    "This should not appear in the current searcher snapshot",
                    "TECH"));
            searcherManager.maybeRefresh(); // refresh happens, but not for this searcher

            TopDocs hitsAfter = searcherRef.search(query, 10);
            System.out.printf("  Same searcher after more adds + refresh: %d docs%n",
                    hitsAfter.totalHits.value);
            System.out.println("  → same count: this searcher is a stable point-in-time snapshot");
        } finally {
            // ALWAYS release — even if an exception is thrown
            searcherManager.release(searcherRef);
            System.out.println("  Searcher released.");
        }

        // After re-acquiring, the new document is now visible
        searcherRef = searcherManager.acquire();
        try {
            Query query  = new TermQuery(new Term("title", "batch"));
            TopDocs hits = searcherRef.search(query, 10);
            System.out.printf("%n  Re-acquired searcher: %d 'batch' docs (new one visible)%n",
                    hits.totalHits.value);
        } finally {
            searcherManager.release(searcherRef);
        }
        System.out.println();
    }

    // ---------------------------------------------------------------
    // SECTION 3: Background refresh thread
    // In production you run maybeRefresh() on a schedule,
    // not manually. This simulates a real production setup
    // where writes happen on one thread and searches on another.
    // ---------------------------------------------------------------
    static void section3_ConcurrentWritesAndReads() throws Exception {
        printHeader("SECTION 3: Background refresh thread simulating production");

        // Schedule maybeRefresh() every 500ms — typical production setting
        // Elasticsearch defaults to index.refresh_interval=1s
        ScheduledExecutorService refresher = Executors.newSingleThreadScheduledExecutor();
        refresher.scheduleAtFixedRate(() -> {
            try {
                searcherManager.maybeRefresh();
            } catch (IOException e) {
                System.err.println("Refresh error: " + e.getMessage());
            }
        }, 0, 500, TimeUnit.MILLISECONDS);

        System.out.println("Background refresh thread started (every 500ms)\\n");
        System.out.println("Simulating: add document, wait, search, observe visibility\\n");

        for (int round = 1; round <= 3; round++) {
            String marker = "CONCURRENT_ROUND_" + round;

            // Write thread: add a document
            writer.addDocument(buildArticle(
                    String.valueOf(docCounter.incrementAndGet()),
                    marker + " Search Systems Article",
                    "Concurrent write and read demonstration for round " + round,
                    "TECH"));

            System.out.printf("  Round %d: document written, waiting for refresh...%n", round);

            // Wait long enough for the background refresher to run
            Thread.sleep(600);

            // Read thread: search for the document
            IndexSearcher s = searcherManager.acquire();
            try {
                Query q     = new TermQuery(new Term("title",
                        "concurrent_round_" + round));
                TopDocs hits = s.search(q, 1);
                System.out.printf("  Round %d: search found %d doc(s) — visible after ~500ms%n",
                        round, hits.totalHits.value);
            } finally {
                searcherManager.release(s);
            }
        }

        refresher.shutdown();
        System.out.println("\\n  This is exactly how Elasticsearch's near-real-time");
        System.out.println("  search works: refresh_interval controls the lag between");
        System.out.println("  indexing and searchability.\\n");
    }

    // ---------------------------------------------------------------
    // SECTION 4: Delete and Update documents
    // Lucene doesn't have a true update — it's delete + add.
    // Deletes are "soft" — the doc is marked deleted in a .liv file
    // but the disk space isn't reclaimed until a merge happens.
    // ---------------------------------------------------------------
    static void section4_DeleteAndUpdate() throws Exception {
        printHeader("SECTION 4: Delete and Update (delete + re-add)");

        // Add a document we'll update
        // IMPORTANT: We commit() here to force the document into a durable segment.
        // Without commit, the doc lives only in a tiny RAM segment that gets
        // merged away immediately when we delete+add — so numDeletedDocs() would be 0.
        String docId = "UPDATE_TEST_001";
        writer.addDocument(buildArticle(docId,
                "Original Title About Search",
                "This is the original body content",
                "TECH"));
        writer.commit();  // Force into a durable segment on disk
        searcherManager.maybeRefresh();

        IndexSearcher s = searcherManager.acquire();
        try {
            Query findById = new TermQuery(new Term("id", docId));
            TopDocs before = s.search(findById, 1);
            System.out.printf("Before update: found %d doc%n", before.totalHits.value);
            if (before.scoreDocs.length > 0) {
                System.out.printf("  Title: %s%n",
                        s.doc(before.scoreDocs[0].doc).get("title"));
            }
        } finally {
            searcherManager.release(s);
        }

        // "Update" = deleteDocuments + addDocument
        // deleteDocuments marks all docs where id=docId as deleted
        writer.deleteDocuments(new Term("id", docId));
        writer.addDocument(buildArticle(docId,
                "Updated Title About Information Retrieval",
                "This is the updated body content after modification",
                "TECH"));
        searcherManager.maybeRefresh();

        s = searcherManager.acquire();
        try {
            Query findById = new TermQuery(new Term("id", docId));
            TopDocs after  = s.search(findById, 2);
            System.out.printf("%nAfter update: found %d doc(s)%n",
                    after.totalHits.value);
            for (ScoreDoc sd : after.scoreDocs) {
                System.out.printf("  Title: %s%n", s.doc(sd.doc).get("title"));
            }
        } finally {
            searcherManager.release(s);
        }

        // Show deleted doc count before merge
        s = searcherManager.acquire();
        try {
            int deletedDocs = s.getIndexReader().numDeletedDocs();
            System.out.printf("%nDeleted docs (not yet reclaimed): %d%n", deletedDocs);
        } finally {
            searcherManager.release(s);
        }
        System.out.println("  Disk space reclaimed after next segment merge.");
        System.out.println("  This is why ES's _delete_by_query doesn't free disk");
        System.out.println("  immediately — you need to forcemerge to reclaim space.\\n");

        writer.commit();
    }

    // ---------------------------------------------------------------
    // HELPERS
    // ---------------------------------------------------------------

    static long countMatching(String field, String term) throws Exception {
        IndexSearcher s = searcherManager.acquire();
        try {
            return searcher(s, field, term);
        } finally {
            searcherManager.release(s);
        }
    }

    static long searcher(IndexSearcher s, String field, String term) throws Exception {
        Query q = new TermQuery(new Term(field, term));
        return s.search(q, 1).totalHits.value;
    }

    static Document buildArticle(String id, String title,
                                  String description, String category) {
        Document doc = new Document();
        doc.add(new StringField("id",       id,          Field.Store.YES));
        doc.add(new TextField("title",      title,       Field.Store.YES));
        doc.add(new TextField("description", description, Field.Store.YES));
        doc.add(new StringField("category", category,    Field.Store.YES));
        doc.add(new StringField("date",     "2024-01-01", Field.Store.YES));
        doc.add(new NumericDocValuesField("date_sort",   20240101));
        doc.add(new SortedDocValuesField("category_facet",
                new BytesRef(category)));
        return doc;
    }

    static void printHeader(String title) {
        System.out.println("\\n━━━ " + title + " ━━━\\n");
    }
}
