package com.minisearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;

public class DatasetIndexer {

    static final String DATASET_PATH = "data/raw/News_Category_Dataset_v3.json";

    public static void main(String[] args) throws IOException {
        System.out.println("╔══════════════════════════════════════════╗");
        System.out.println("║        DATASET INDEXER - DAY 3           ║");
        System.out.println("╚══════════════════════════════════════════╝\\n");

        long startTime = System.currentTimeMillis();

        Directory directory = IndexConfig.openDiskDirectory();
        EnglishAnalyzer analyzer = new EnglishAnalyzer();

        // ---------------------------------------------------------------
        // IndexWriterConfig tuning — these settings matter at scale
        // ---------------------------------------------------------------
        IndexWriterConfig config = new IndexWriterConfig(analyzer);

        // CREATE wipes the existing index on open.
        // Change to CREATE_OR_APPEND if you want to add to an existing index.
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        // RAM buffer: Lucene accumulates documents here before flushing to disk.
        // Larger buffer = fewer segments = faster indexing = more RAM used.
        // 256MB is a good starting point for bulk indexing.
        config.setRAMBufferSizeMB(16);

        IndexWriter writer = new IndexWriter(directory, config);
        ObjectMapper mapper = new ObjectMapper();

        int indexed = 0;
        int skipped = 0;

        try (BufferedReader reader = new BufferedReader(
                new FileReader(Paths.get(DATASET_PATH).toFile()))) {

            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                try {
                    JsonNode node = mapper.readTree(line);
                    Document doc = buildDocument(node);
                    writer.addDocument(doc);
                    indexed++;

                    // Progress reporting every 10k docs
                    if (indexed % 10_000 == 0) {
                        long elapsed = System.currentTimeMillis() - startTime;
                        System.out.printf("  Indexed %,6d docs  |  %.1f sec  |  %.0f docs/sec%n",
                                indexed, elapsed / 1000.0,
                                indexed / (elapsed / 1000.0));
                    }
                } catch (Exception e) {
                    skipped++;
                    // In production: log and continue, never crash the indexer
                    // on a single bad document
                }
            }
        }

        // ---------------------------------------------------------------
        // Commit flushes all buffered documents and writes segments_N.
        // Without this, nothing is visible to any IndexReader.
        // forceMerge(1) merges all segments into one — good for a
        // read-heavy index that won't be updated, but slow to run.
        // Skip forceMerge for now and add it as an experiment below.
        // ---------------------------------------------------------------
        System.out.println("\\nCommitting...");
        writer.commit();

        long elapsed = System.currentTimeMillis() - startTime;
        System.out.printf("%n✓ Indexed:  %,d documents%n", indexed);
        System.out.printf("✗ Skipped:  %,d documents%n", skipped);
        System.out.printf("⏱ Time:     %.2f seconds%n", elapsed / 1000.0);
        System.out.printf("⚡ Rate:     %.0f docs/sec%n", indexed / (elapsed / 1000.0));

        // Print segment count before close — useful to understand merge behavior
        // Use NRT reader from writer since IndexWriter.getSegmentCount() is package-private
        try (IndexReader reader = DirectoryReader.open(writer)) {
            System.out.printf("⎇ Segments: %d%n", reader.leaves().size());
        }

        writer.close();
        analyzer.close();
        directory.close();

        System.out.println("\\nIndex written to: " + IndexConfig.INDEX_PATH);
        System.out.println("Run IndexInspector to explore the index.");
    }

    // ---------------------------------------------------------------
    // This method is the heart of the indexer.
    // Every field decision here has consequences for search behavior.
    // Read each field's comment carefully.
    // ---------------------------------------------------------------
    static Document buildDocument(JsonNode node) {
        Document doc = new Document();

        // --- ID field ---
        // StringField: NOT analyzed, stored.
        // Use for exact-match lookups and deduplication.
        // Never use TextField for IDs — StandardAnalyzer would shred
        // "user-12345" into ["user", "12345"].
        // JSON field: "link" → index field: "id"
        doc.add(new StringField("id",
                getOrDefault(node, "link", "unknown"),
                Field.Store.YES));

        // --- Title field ---
        // TextField: analyzed with EnglishAnalyzer (stemming + stop words).
        // Stored so we can display it in search results.
        // This is the primary full-text search field.
        // JSON field: "headline" → index field: "title"
        doc.add(new TextField("title",
                getOrDefault(node, "headline", ""),
                Field.Store.YES));

        // --- Description field ---
        // TextField: analyzed, stored.
        // Longer text — will have more terms and more relevance signal.
        // JSON field: "short_description" → index field: "description"
        doc.add(new TextField("description",
                getOrDefault(node, "short_description", ""),
                Field.Store.YES));

        // --- Author field ---
        // StringField: NOT analyzed, stored.
        // Authors are proper nouns — you want exact match ("Sarah Chen"),
        // not stemmed partial matches ("sarah", "chen" separately).
        // JSON field: "authors" → index field: "author"
        doc.add(new StringField("author",
                getOrDefault(node, "authors", "unknown"),
                Field.Store.YES));

        // --- Category field ---
        // StringField: NOT analyzed, stored.
        // Categories are controlled vocabulary — exact match only.
        // "TECHNOLOGY" should not stem to "technolog".
        doc.add(new StringField("category",
                getOrDefault(node, "category", "UNKNOWN"),
                Field.Store.YES));

        // --- Date as string field ---
        // StringField: stored for display.
        doc.add(new StringField("date",
                getOrDefault(node, "date", ""),
                Field.Store.YES));

        // --- Date as integer for range queries ---
        // IntPoint: enables fast numeric range queries.
        // NOT stored by default — IntPoint is index-only.
        // We add a companion StoredField to retrieve the value.
        //
        // WHY TWO FIELDS for date?
        // IntPoint is optimized for range queries but can't be retrieved.
        // StoredField can be retrieved but can't be range-queried.
        // This is a common Lucene pattern: one field for searching,
        // one for retrieval.
        //
        // Parse date string "YYYY-MM-DD" to integer YYYYMMDD for range queries
        int dateInt = 0;
        String dateStr = getOrDefault(node, "date", "");
        if (!dateStr.isEmpty()) {
            try {
                // Convert "2022-09-23" to 20220923
                dateInt = Integer.parseInt(dateStr.replace("-", ""));
            } catch (NumberFormatException e) {
                // Keep default 0 if date format is unexpected
            }
        }
        doc.add(new IntPoint("date_int", dateInt));
        doc.add(new StoredField("date_int_stored", dateInt));

        // --- NumericDocValuesField for sorting by date ---
        // DocValues are a column-oriented structure for fast sorting and faceting.
        // Without this, sorting by date requires loading all stored field values
        // at search time — very slow at scale.
        // With this, Lucene reads a compact column of longs directly.
        // You'll use this on Day 5 for sort-by-date.
        doc.add(new NumericDocValuesField("date_sort", dateInt));

        // --- SortedDocValuesField for category faceting ---
        // Enables fast group-by / facet counts on the category field.
        // You'll use this on Day 6 for faceted search.
        doc.add(new SortedDocValuesField("category_facet",
                new org.apache.lucene.util.BytesRef(
                        getOrDefault(node, "category", "UNKNOWN"))));

        return doc;
    }

    static String getOrDefault(JsonNode node, String field, String defaultValue) {
        return node.has(field) && !node.get(field).isNull()
                ? node.get(field).asText().trim()
                : defaultValue;
    }
}