package com.minisearch;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;

public class ScoringExplorer {

    static DirectoryReader reader;
    static IndexSearcher   searcher;
    static Analyzer        analyzer;

    public static void main(String[] args) throws Exception {
        Directory directory = IndexConfig.openDiskDirectory();
        reader   = DirectoryReader.open(directory);
        analyzer = new EnglishAnalyzer();
        searcher = new IndexSearcher(reader);

        // Default similarity is BM25 — we make it explicit here
        // so you can swap it out in experiments
        searcher.setSimilarity(new BM25Similarity());

        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.println("║         SCORING EXPLORER - DAY 5                    ║");
        System.out.println("╚══════════════════════════════════════════════════════╝");
        System.out.printf("%nIndex: %,d documents%n%n", reader.numDocs());

        section1_ExplainScores();
        section2_IdfEffect();
        section3_DocumentLengthEffect();
        section4_FieldBoosting();
        section5_BoostQuery();
        section6_BM25vsTFIDF();
        section7_CustomBM25Params();

        reader.close();
        directory.close();
        analyzer.close();
    }

    // ---------------------------------------------------------------
    // SECTION 1: Score Explanation
    // explain() returns the full arithmetic breakdown of why a
    // document scored the way it did.
    // This is the single most useful debugging tool in Lucene.
    // ---------------------------------------------------------------
    static void section1_ExplainScores() throws Exception {
        printHeader("SECTION 1: Score Explanation — why did this document score this way?");

        Query query = new TermQuery(new Term("description", "search"));
        TopDocs results = searcher.search(query, 3);

        System.out.println("Query: description:\"search\"\\n");
        System.out.println("Top 3 results with full score explanation:\\n");

        for (ScoreDoc sd : results.scoreDocs) {
            Document doc = searcher.doc(sd.doc);
            System.out.printf("DocID=%-5d  score=%.4f  title=\"%s\"%n",
                    sd.doc, sd.score, doc.get("title"));

            // explain() computes the score breakdown for one specific doc
            Explanation explanation = searcher.explain(query, sd.doc);
            printExplanation(explanation, 1);
            System.out.println();
        }

        // Now explain a non-matching document to understand what 0 means
        System.out.println("Explanation for a non-matching document (docID=0):");
        Explanation noMatch = searcher.explain(query, 0);
        printExplanation(noMatch, 1);
        System.out.println();
    }

    // ---------------------------------------------------------------
    // SECTION 2: IDF Effect
    // Rare terms score higher than common terms.
    // Compare the IDF component across terms with different frequencies.
    // ---------------------------------------------------------------
    static void section2_IdfEffect() throws Exception {
        printHeader("SECTION 2: IDF Effect — rare terms score higher");

        // These terms have very different document frequencies
        // Common term: appears in many docs → low IDF
        // Rare term:   appears in few docs  → high IDF
        String[] terms = {"search", "index", "lucen", "quantum", "bibliograph"};

        System.out.println("For a single document, IDF contribution per term:\\n");
        System.out.printf("  %-20s  %8s  %8s  %10s%n",
                "Term", "Doc Freq", "IDF est.", "Score(doc0)");
        System.out.println("  " + "─".repeat(52));

        for (String term : terms) {
            Query q = new TermQuery(new Term("description", term));
            TopDocs results = searcher.search(q, 1);
            long df = results.totalHits.value;

            if (df > 0) {
                Explanation exp = searcher.explain(q, results.scoreDocs[0].doc);
                // Extract IDF value from explanation tree
                float idf = extractIdf(exp);
                System.out.printf("  %-20s  %8d  %8.4f  %10.4f%n",
                        term, df, idf, results.scoreDocs[0].score);
            } else {
                System.out.printf("  %-20s  %8d  %8s  %10s%n",
                        term, 0, "N/A", "no hits");
            }
        }
        System.out.println();
        System.out.println("  Observation: as doc frequency drops, IDF rises.");
        System.out.println("  A term in 10 docs scores much higher than one in 10,000.\\n");
    }

    // ---------------------------------------------------------------
    // SECTION 3: Document Length Effect
    // BM25 normalizes for document length.
    // A short document with one mention of a term scores higher
    // than a long document with one mention of the same term.
    // ---------------------------------------------------------------
    static void section3_DocumentLengthEffect() throws Exception {
        printHeader("SECTION 3: Document Length Normalization");

        // Search for a specific term and compare how document length
        // affects the score across the top results
        Query query = new TermQuery(new Term("description", "search"));
        TopDocs results = searcher.search(query, 10);

        System.out.println("Query: description:\"search\"");
        System.out.println("Comparing score vs approximate document length:\\n");
        System.out.printf("  %-5s  %-7s  %-10s  %-8s  %s%n",
                "Rank", "Score", "description Length", "TF", "description (truncated)");
        System.out.println("  " + "─".repeat(70));

        int rank = 1;
        for (ScoreDoc sd : results.scoreDocs) {
            Document doc  = searcher.doc(sd.doc);
            String description   = doc.get("description") != null ? doc.get("description") : "";
            int descriptionLen   = description.split("\\s+").length;

            // Get TF from explanation
            Explanation exp = searcher.explain(query, sd.doc);
            float tf = extractTf(exp);

            System.out.printf("  %-5d  %-7.4f  %-10d  %-8.4f  %s%n",
                    rank++, sd.score, descriptionLen, tf,
                    truncate(doc.get("description"), 80));
        }
        System.out.println();
        System.out.println("  Observation: shorter documents with the same term");
        System.out.println("  frequency tend to score higher due to length normalization.");
        System.out.println("  This is controlled by the b parameter in BM25.\\n");
    }

    // ---------------------------------------------------------------
    // SECTION 4: Field Boosting at Index Time vs Query Time
    //
    // There are TWO places you can apply boosts:
    //
    // 1. QUERY TIME (recommended) — wrap query in BoostQuery
    //    Flexible: change boost without reindexing
    //    This is what you'll use in production
    //
    // 2. INDEX TIME — was supported in Lucene <7, now removed
    //    Not available in Lucene 9 — mentioned so you know why
    //    old blog posts you find online don't compile anymore
    //
    // The standard pattern: search multiple fields with different
    // boosts using BooleanQuery + BoostQuery
    // ---------------------------------------------------------------
    static void section4_FieldBoosting() throws Exception {
        printHeader("SECTION 4: Field Boosting — title matches outrank description matches");

        String searchTerm = "search";

        // Baseline: description only, no boost
        Query descriptionOnly = new TermQuery(new Term("description", searchTerm));
        System.out.println("Baseline — description only:");
        printTopN(descriptionOnly, 5);

        // Title only, no boost
        Query titleOnly = new TermQuery(new Term("title", searchTerm));
        System.out.println("Title only (no boost):");
        printTopN(titleOnly, 5);

        // Combined: title boosted 3x over description
        // BoostQuery multiplies the score of its wrapped query by the boost factor
        Query combined = new BooleanQuery.Builder()
                .add(new BoostQuery(
                        new TermQuery(new Term("title", searchTerm)), 3.0f),
                        BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term("description", searchTerm)),
                        BooleanClause.Occur.SHOULD)
                .build();
        System.out.println("Combined: title(3x boost) + description(1x):");
        printTopN(combined, 5);

        // Show the score breakdown for the top result
        TopDocs top = searcher.search(combined, 1);
        if (top.scoreDocs.length > 0) {
            System.out.println("\\nScore breakdown for top result:");
            Explanation exp = searcher.explain(combined, top.scoreDocs[0].doc);
            printExplanation(exp, 1);
        }
        System.out.println();
    }

    // ---------------------------------------------------------------
    // SECTION 5: BoostQuery in depth
    // BoostQuery wraps any query and multiplies its score.
    // You can boost specific terms, phrases, or entire sub-queries.
    // ---------------------------------------------------------------
    static void section5_BoostQuery() throws Exception {
        printHeader("SECTION 5: BoostQuery — fine-grained relevance tuning");

        // Scenario: user searches "lucene search"
        // We want documents where BOTH terms appear to rank highest,
        // documents with "lucene" in title to rank next,
        // and any document with either term to appear in results.
        Query q = new BooleanQuery.Builder()
                // Strong signal: both terms in title
                .add(new BoostQuery(
                        new BooleanQuery.Builder()
                            .add(new TermQuery(new Term("title", "task")),
                                    BooleanClause.Occur.MUST)
                            .add(new TermQuery(new Term("title", "search")),
                                    BooleanClause.Occur.MUST)
                            .build(),
                        5.0f),   // 5x boost
                        BooleanClause.Occur.SHOULD)
                // Medium signal: lucene in title
                .add(new BoostQuery(
                        new TermQuery(new Term("title", "task")), 3.0f),
                        BooleanClause.Occur.SHOULD)
                // Weak signal: terms in description
                .add(new TermQuery(new Term("description", "task")),
                        BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term("description", "search")),
                        BooleanClause.Occur.SHOULD)
                .build();

        System.out.println("Multi-signal boost query for 'task search':");
        System.out.println("  title:(task AND search) × 5.0");
        System.out.println("  title:task              × 3.0");
        System.out.println("  description:task               × 1.0");
        System.out.println("  description:search               × 1.0\\n");
        printTopNWithScore(q, 8);

        // Recency boost pattern — newer articles score higher
        // Combine text score with a date-based boost
        // You'll build this more precisely on Day 6 with FunctionScoreQuery
        System.out.println("\\nRecency boost: TECHNOLOGY articles about search,");
        System.out.println("boosting more recent dates:\\n");

        Query recentTech = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("description", "search")),
                        BooleanClause.Occur.MUST)
                .add(new BoostQuery(
                        new TermQuery(new Term("category", "TECH")), 2.0f),
                        BooleanClause.Occur.SHOULD)
                .build();
        printTopNWithScore(recentTech, 5);
    }

    // ---------------------------------------------------------------
    // SECTION 6: BM25 vs Classic TF-IDF
    // Swap the similarity and observe how results and scores change.
    // The order may be similar but the scores will differ.
    // The saturation effect is most visible on documents with
    // high term frequency.
    // ---------------------------------------------------------------
    static void section6_BM25vsTFIDF() throws Exception {
        printHeader("SECTION 6: BM25 vs Classic TF-IDF");

        Query query = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("description", "search")),
                        BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term("description", "index")),
                        BooleanClause.Occur.SHOULD)
                .build();

        // BM25 (default)
        searcher.setSimilarity(new BM25Similarity());
        TopDocs bm25Results = searcher.search(query, 5);
        System.out.println("BM25 (default) — description:search OR description:index:\\n");
        System.out.printf("  %-5s  %-8s  %s%n", "Rank", "Score", "Title");
        System.out.println("  " + "─".repeat(60));
        int rank = 1;
        for (ScoreDoc sd : bm25Results.scoreDocs) {
            Document doc = searcher.doc(sd.doc);
            System.out.printf("  %-5d  %-8.4f  %s%n",
                    rank++, sd.score, doc.get("title"));
        }

        // Classic TF-IDF
        searcher.setSimilarity(new ClassicSimilarity());
        TopDocs tfidfResults = searcher.search(query, 5);
        System.out.println("\\nClassic TF-IDF — same query:\\n");
        System.out.printf("  %-5s  %-8s  %s%n", "Rank", "Score", "Title");
        System.out.println("  " + "─".repeat(60));
        rank = 1;
        for (ScoreDoc sd : tfidfResults.scoreDocs) {
            Document doc = searcher.doc(sd.doc);
            System.out.printf("  %-5d  %-8.4f  %s%n",
                    rank++, sd.score, doc.get("title"));
        }

        System.out.println();
        System.out.println("  Observations to make:");
        System.out.println("  1. Are the rankings identical or different?");
        System.out.println("  2. Are the absolute score values different?");
        System.out.println("  3. BM25 scores tend to be smaller but more stable");
        System.out.println("     across documents of different lengths.");
        System.out.println();

        // Reset to BM25 for remaining sections
        searcher.setSimilarity(new BM25Similarity());
    }

    // ---------------------------------------------------------------
    // SECTION 7: Custom BM25 Parameters
    // k1 controls TF saturation. b controls length normalization.
    // Tuning these is how you adapt BM25 to your specific content.
    // ---------------------------------------------------------------
    static void section7_CustomBM25Params() throws Exception {
        printHeader("SECTION 7: Custom BM25 Parameters — k1 and b tuning");

        Query query = new TermQuery(new Term("description", "search"));

        System.out.println("Same query with different BM25 parameters:\\n");
        System.out.printf("  %-25s  %-6s  %-6s  %-8s  %-8s  %-8s%n",
                "Configuration", "k1", "b", "Score#1", "Score#2", "Score#3");
        System.out.println("  " + "─".repeat(70));

        float[][] params = {
            {1.2f, 0.75f},  // Lucene default
            {2.0f, 0.75f},  // higher k1: TF has more influence, slower saturation
            {0.5f, 0.75f},  // lower k1:  TF saturates faster, more like binary
            {1.2f, 0.0f},   // b=0: no length normalization
            {1.2f, 1.0f},   // b=1: full length normalization
        };
        String[] labels = {
            "Default",
            "k1=2.0 (more TF)",
            "k1=0.5 (less TF)",
            "b=0.0 (no length norm)",
            "b=1.0 (full length norm)",
        };

        for (int i = 0; i < params.length; i++) {
            float k1 = params[i][0];
            float b  = params[i][1];
            searcher.setSimilarity(new BM25Similarity(k1, b));
            TopDocs results = searcher.search(query, 3);
            float s1 = results.scoreDocs.length > 0 ? results.scoreDocs[0].score : 0;
            float s2 = results.scoreDocs.length > 1 ? results.scoreDocs[1].score : 0;
            float s3 = results.scoreDocs.length > 2 ? results.scoreDocs[2].score : 0;
            System.out.printf("  %-25s  %-6.1f  %-6.2f  %-8.4f  %-8.4f  %-8.4f%n",
                    labels[i], k1, b, s1, s2, s3);
        }

        System.out.println();
        System.out.println("  k1 guidance:");
        System.out.println("    Short fields (title, tags): lower k1 (0.5-1.0)");
        System.out.println("    Long fields (body, description): higher k1 (1.2-2.0)");
        System.out.println();
        System.out.println("  b guidance:");
        System.out.println("    Uniform length content: b=0.75 (default)");
        System.out.println("    Highly variable length: b=1.0");
        System.out.println("    Title / short fields:   b=0.0-0.3");
        System.out.println();

        // Reset to default
        searcher.setSimilarity(new BM25Similarity());
    }

    // ---------------------------------------------------------------
    // HELPERS
    // ---------------------------------------------------------------

    static void printExplanation(Explanation exp, int depth) {
        String indent = "  ".repeat(depth);
        String match  = exp.isMatch() ? "✓" : "✗";
        System.out.printf("%s%s %.4f  %s%n",
                indent, match, exp.getValue().doubleValue(), exp.getDescription());
        for (Explanation detail : exp.getDetails()) {
            printExplanation(detail, depth + 1);
        }
    }

    static void printTopN(Query query, int n) throws Exception {
        TopDocs results = searcher.search(query, n);
        for (ScoreDoc sd : results.scoreDocs) {
            Document doc = searcher.doc(sd.doc);
            System.out.printf("  [%.4f] %s%n", sd.score, doc.get("title"));
        }
        System.out.println();
    }

    static void printTopNWithScore(Query query, int n) throws Exception {
        TopDocs results = searcher.search(query, n);
        System.out.printf("  %-8s  %-10s  %s%n", "Score", "Category", "Title");
        System.out.println("  " + "─".repeat(65));
        for (ScoreDoc sd : results.scoreDocs) {
            Document doc = searcher.doc(sd.doc);
            System.out.printf("  %-8.4f  %-10s  %s%n",
                    sd.score,
                    doc.get("category"),
                    truncate(doc.get("title"), 45));
        }
        System.out.println();
    }

    // Extract IDF value from explanation tree by walking the tree
    static float extractIdf(Explanation exp) {
        if (exp.getDescription().startsWith("idf")) {
            return exp.getValue().floatValue();
        }
        for (Explanation detail : exp.getDetails()) {
            float val = extractIdf(detail);
            if (val > 0) return val;
        }
        return 0f;
    }

    // Extract TF value from explanation tree
    static float extractTf(Explanation exp) {
        if (exp.getDescription().startsWith("tf")) {
            return exp.getValue().floatValue();
        }
        for (Explanation detail : exp.getDetails()) {
            float val = extractTf(detail);
            if (val > 0) return val;
        }
        return 0f;
    }

    static String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() > max ? s.substring(0, max - 3) + "..." : s;
    }

    static void printHeader(String title) {
        System.out.println("\\n━━━ " + title + " ━━━\\n");
    }
}
