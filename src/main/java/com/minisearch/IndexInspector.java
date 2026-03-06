package com.minisearch;

import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IndexInspector {

    public static void main(String[] args) throws IOException {
        Directory directory = IndexConfig.openDiskDirectory();
        DirectoryReader reader = DirectoryReader.open(directory);

        printSummary(reader);
        printAllDocuments(reader);
        printTermDictionary(reader, "description", 20);
        printTermDictionary(reader, "title", 20);
        printTermDictionary(reader, "category", 20);
        printPostingsList(reader, "description", "china");
        printSegmentInfo(reader);

        reader.close();
        directory.close();
    }

    // ---------------------------------------------------------------
    // High-level index summary
    // ---------------------------------------------------------------
    static void printSummary(DirectoryReader reader) {
        System.out.println("╔══════════════════════════════════════╗");
        System.out.println("║         INDEX SUMMARY                ║");
        System.out.println("╚══════════════════════════════════════╝");
        System.out.printf("  Total documents (including deleted): %d%n", reader.maxDoc());
        System.out.printf("  Live documents:                      %d%n", reader.numDocs());
        System.out.printf("  Deleted documents:                   %d%n", reader.numDeletedDocs());
        System.out.printf("  Number of segments:                  %d%n", reader.leaves().size());
        System.out.printf("  Index version:                       %d%n%n", reader.getVersion());
    }

    // ---------------------------------------------------------------
    // Walk every document and print its stored fields
    // This is what Luke's Documents tab shows you
    // ---------------------------------------------------------------
    static void printAllDocuments(DirectoryReader reader) throws IOException {
        System.out.println("━━━ ALL STORED DOCUMENTS ━━━\n");
        IndexSearcher searcher = new IndexSearcher(reader);

        for (int docId = 0; docId < reader.maxDoc(); docId++) {
            // Skip deleted documents
            if (reader.hasDeletions()) {
                // In production you'd use Bits to check live docs
            }
            var doc = reader.storedFields().document(docId);
            System.out.printf("  DocID %-3d │", docId);
            for (var field : doc.getFields()) {
                System.out.printf(" %s=\"%s\"", field.name(), field.stringValue());
            }
            System.out.println();
        }
        System.out.println();
    }

    // ---------------------------------------------------------------
    // Print the term dictionary for a field — the raw inverted index
    // This shows you exactly what tokens Lucene stored at index time
    // ---------------------------------------------------------------
    static void printTermDictionary(DirectoryReader reader, String field, int limit)
            throws IOException {
        System.out.printf("━━━ TERM DICTIONARY: field=\"%s\" (top %d by doc freq) ━━━%n%n",
                field, limit);
        System.out.printf("  %-25s  %8s  %12s%n", "Term", "Doc Freq", "Total TF");
        System.out.println("  " + "─".repeat(50));

        // Collect terms and their frequencies
        Map<String, long[]> termStats = new HashMap<>();
        for (LeafReaderContext ctx : reader.leaves()) {
            Terms terms = ctx.reader().terms(field);
            if (terms == null) continue;

            TermsEnum te = terms.iterator();
            BytesRef termBytes;
            while ((termBytes = te.next()) != null) {
                String termStr = termBytes.utf8ToString();
                long docFreq = te.docFreq();
                long totalTF = te.totalTermFreq();
                termStats.merge(termStr, new long[]{docFreq, totalTF},
                        (a, b) -> new long[]{a[0] + b[0], a[1] + b[1]});
            }
        }

        // Sort by doc frequency descending, print top N
        termStats.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue()[0], a.getValue()[0]))
                .limit(limit)
                .forEach(e -> System.out.printf("  %-25s  %8d  %12d%n",
                        e.getKey(), e.getValue()[0], e.getValue()[1]));
        System.out.println();
    }

    // ---------------------------------------------------------------
    // Print the postings list for a specific term
    // A postings list = the list of documents that contain a term,
    // with their positions. This is the core data structure of the index.
    // ---------------------------------------------------------------
    static void printPostingsList(DirectoryReader reader, String field, String termText)
            throws IOException {
        System.out.printf("━━━ POSTINGS LIST: %s:\"%s\" ━━━%n%n", field, termText);
        System.out.printf("  %-10s  %-10s  %s%n", "DocID", "Term Freq", "Positions");
        System.out.println("  " + "─".repeat(50));

        Term term = new Term(field, termText);
        for (LeafReaderContext ctx : reader.leaves()) {
            Terms terms = ctx.reader().terms(field);
            if (terms == null) continue;

            TermsEnum te = terms.iterator();
            if (!te.seekExact(term.bytes())) {
                System.out.println("  Term not found in index.");
                continue;
            }

            // PostingsEnum gives you doc IDs, term frequencies, and positions
            PostingsEnum postings = te.postings(null, PostingsEnum.POSITIONS);
            int docId;
            while ((docId = postings.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
                int globalDocId = ctx.docBase + docId;
                int freq = postings.freq();
                StringBuilder positions = new StringBuilder();
                for (int i = 0; i < freq; i++) {
                    if (i > 0) positions.append(", ");
                    positions.append(postings.nextPosition());
                }
                // Retrieve stored field to show the doc title
                var doc = reader.storedFields().document(globalDocId);
                String title = doc.get("title");
                System.out.printf("  %-10d  %-10d  pos=[%s]  title=\"%s\"%n",
                        globalDocId, freq, positions, title);
            }
        }
        System.out.println();
    }

    // ---------------------------------------------------------------
    // Print per-segment breakdown
    // Each segment is an immutable mini-index. Understanding segments
    // is key to understanding Lucene's write/merge performance model.
    // ---------------------------------------------------------------
    static void printSegmentInfo(DirectoryReader reader) {
        System.out.println("━━━ SEGMENT BREAKDOWN ━━━\n");
        System.out.printf("  %-10s  %-8s  %-8s%n", "Segment", "Docs", "Deleted");
        System.out.println("  " + "─".repeat(32));

        for (LeafReaderContext ctx : reader.leaves()) {
            LeafReader leaf = ctx.reader();
            System.out.printf("  %-10s  %-8d  %-8d%n",
                    leaf.toString().substring(0, Math.min(10, leaf.toString().length())),
                    leaf.numDocs(),
                    leaf.numDeletedDocs());
        }
        System.out.println();
    }
}
