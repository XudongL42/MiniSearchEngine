package com.minisearch;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AnalyzerPlayground {

    // ---------------------------------------------------------------
    // Test sentences — deliberately chosen to expose analyzer differences
    // ---------------------------------------------------------------
    static final String[] TEST_INPUTS = {
        "Lucene is running faster than ever",           // stemming: running→run, faster→fast
        "The quick brown fox jumps over the lazy dog",  // stop words: the, over
        "user@example.com visited example.com/search",  // punctuation handling
        "Don't won't can't I've they're",               // contractions
        "Java XML HTTP REST API UUID",                  // acronyms / all-caps
        "Cats are better than dogs, aren't they?",      // stemming + punctuation
        "2024 was a great year for LLMs and AI",        // numbers + acronyms
    };

    public static void main(String[] args) throws IOException {

        // Build an ordered map of name → analyzer
        Map<String, Analyzer> analyzers = new LinkedHashMap<>();
        analyzers.put("StandardAnalyzer",   new StandardAnalyzer());
        analyzers.put("EnglishAnalyzer",    new EnglishAnalyzer());
        analyzers.put("WhitespaceAnalyzer", new WhitespaceAnalyzer());
        analyzers.put("SimpleAnalyzer",     new SimpleAnalyzer());
        analyzers.put("KeywordAnalyzer",    new KeywordAnalyzer());

        System.out.println("╔══════════════════════════════════════════════════════════╗");
        System.out.println("║           LUCENE ANALYZER PLAYGROUND - DAY 2             ║");
        System.out.println("╚══════════════════════════════════════════════════════════╝");

        // ---------------------------------------------------------------
        // Section 1: Side-by-side token comparison across all analyzers
        // This is the most important view — it shows divergence clearly
        // ---------------------------------------------------------------
        System.out.println("\\n━━━ SECTION 1: Side-by-side token comparison ━━━\\n");
        for (String input : TEST_INPUTS) {
            printSideBySide(input, analyzers);
        }

        // ---------------------------------------------------------------
        // Section 2: Deep token inspection — all attributes on each token
        // Offset = character positions in original string (for highlighting)
        // PositionIncrement = gap between tokens (1=adjacent, >1=gap after stop word removal)
        // Type = token type assigned by the tokenizer
        // ---------------------------------------------------------------
        System.out.println("\\n━━━ SECTION 2: Deep token attribute inspection ━━━\\n");
        String deepInput = "The running cats are eating quickly";
        System.out.println("Input: \\" + deepInput + "\\");
        System.out.println();

        printDeepTokens("StandardAnalyzer", new StandardAnalyzer(), deepInput);
        printDeepTokens("EnglishAnalyzer",  new EnglishAnalyzer(),  deepInput);

        // ---------------------------------------------------------------
        // Section 3: The index-time vs query-time mismatch demo
        // This is the most important practical lesson of the day
        // ---------------------------------------------------------------
        System.out.println("\\n━━━ SECTION 3: Index-time vs Query-time mismatch demo ━━━\\n");
        demonstrateMismatch(analyzers);

        // ---------------------------------------------------------------
        // Section 4: Stemming deep dive
        // See exactly which words collapse to the same stem
        // ---------------------------------------------------------------
        System.out.println("\\n━━━ SECTION 4: Stemming — words that collapse to the same stem ━━━\\n");
        String[] stemmingGroups = {
            "run runs running runner ran",
            "index indexes indexed indexing indexer",
            "search searches searched searching searcher",
            "analyze analysis analyzer analyzers analyzing",
            "fast faster fastest",
            "good better best",       // irregular — watch what happens
        };

        EnglishAnalyzer english = new EnglishAnalyzer();
        for (String group : stemmingGroups) {
            System.out.printf("Input:  %s%n", group);
            System.out.printf("Stems:  %s%n%n", getTokens(english, "field", group));
        }

        // Close all analyzers
        analyzers.values().forEach(Analyzer::close);
        english.close();

        // Simulate what happens if you accidentally analyze an ID field
        KeywordAnalyzer kw = new KeywordAnalyzer();
        StandardAnalyzer std = new StandardAnalyzer();
        String[] ids = { "user-12345", "ORDER_9981", "tx:ABC.001" };
        System.out.println("\\n━━━ Experiment C: ID field analyzer comparison ━━━\\n");
        for (String id : ids) {
            System.out.printf("ID: %-20s  Keyword: %-20s  Standard: %s%n",
                id,
                getTokens(kw, "id", id),
                getTokens(std, "id", id));
        }
        kw.close();
        std.close();
    }

    // ---------------------------------------------------------------
    // Prints a side-by-side comparison table for one input across all analyzers
    // ---------------------------------------------------------------
    static void printSideBySide(String input, Map<String, Analyzer> analyzers) throws IOException {
        System.out.println("┌─ Input: \\" + input + "\\");

        int maxWidth = 20;
        for (Map.Entry<String, Analyzer> entry : analyzers.entrySet()) {
            List<String> tokens = getTokens(entry.getValue(), "field", input);
            String tokenStr = String.join(", ", tokens);

            // Truncate if too long for display
            if (tokenStr.length() > 80) {
                tokenStr = tokenStr.substring(0, 77) + "...";
            }
            System.out.printf("│  %-22s → [%s]%n", entry.getKey(), tokenStr);
        }
        System.out.println();
    }

    // ---------------------------------------------------------------
    // Prints full attribute detail for every token from one analyzer
    // ---------------------------------------------------------------
    static void printDeepTokens(String name, Analyzer analyzer, String input) throws IOException {
        System.out.println("  Analyzer: " + name);
        System.out.printf("  %-5s  %-20s  %-12s  %-10s  %-15s%n",
                "Pos", "Term", "Offset", "PosIncr", "Type");
        System.out.println("  " + "─".repeat(70));

        try (TokenStream stream = analyzer.tokenStream("field", input)) {
            CharTermAttribute      termAttr   = stream.addAttribute(CharTermAttribute.class);
            OffsetAttribute        offsetAttr = stream.addAttribute(OffsetAttribute.class);
            PositionIncrementAttribute posAttr = stream.addAttribute(PositionIncrementAttribute.class);
            TypeAttribute          typeAttr   = stream.addAttribute(TypeAttribute.class);

            stream.reset();
            int position = 0;
            while (stream.incrementToken()) {
                position += posAttr.getPositionIncrement();
                System.out.printf("  %-5d  %-20s  %-12s  %-10d  %-15s%n",
                        position,
                        termAttr.toString(),
                        offsetAttr.startOffset() + "-" + offsetAttr.endOffset(),
                        posAttr.getPositionIncrement(),
                        typeAttr.type());
            }
            stream.end();
        }
        System.out.println();
    }

    // ---------------------------------------------------------------
    // Demonstrates the mismatch problem: indexing with one analyzer,
    // then manually constructing TermQueries (no query-time analysis).
    // This is exactly what happens when you use TermQuery in production.
    // ---------------------------------------------------------------
    static void demonstrateMismatch(Map<String, Analyzer> analyzers) throws IOException {
        String[] queryAttempts = { "Running", "running", "run", "RUNNING" };
        String indexedText = "The service is running smoothly";

        System.out.println("Document text: \\" + indexedText + "\\");
        System.out.println();

        // Show what gets stored in the index under each analyzer
        for (Map.Entry<String, Analyzer> entry : analyzers.entrySet()) {
            List<String> indexed = getTokens(entry.getValue(), "body", indexedText);
            System.out.printf("%-22s indexes to: %s%n", entry.getKey(), indexed);
        }

        System.out.println();
        System.out.println("If you use TermQuery (no analysis) with these query strings:");
        System.out.println();

        // For StandardAnalyzer and EnglishAnalyzer, show which TermQuery inputs would match
        String[] targetAnalyzers = { "StandardAnalyzer", "EnglishAnalyzer" };
        for (String analyzerName : targetAnalyzers) {
            Analyzer a = analyzers.get(analyzerName);
            List<String> indexedTokens = getTokens(a, "body", indexedText);
            System.out.println("  " + analyzerName + " index contains: " + indexedTokens);
            for (String attempt : queryAttempts) {
                boolean wouldMatch = indexedTokens.contains(attempt)
                    || indexedTokens.contains(attempt.toLowerCase());
                // For English, stemmed form
                List<String> queryTokens = getTokens(a, "body", attempt);
                boolean stemMatch = !queryTokens.isEmpty() && indexedTokens.contains(queryTokens.get(0));
                System.out.printf("    TermQuery(body, %-12s → raw match=%-5s  QueryParser would match=%-5s%n",
                        "\\" + attempt + "\\)",
                        indexedTokens.contains(attempt),
                        stemMatch);
            }
            System.out.println();
        }

        System.out.println("  KEY INSIGHT: TermQuery never analyzes. QueryParser always analyzes.");
        System.out.println("  Always match your query-time strategy to your index-time analyzer.\\n");
    }

    // ---------------------------------------------------------------
    // Core utility: tokenize a string with an analyzer, return token list
    // Reuse this in every future file — it's your debugging swiss army knife
    // ---------------------------------------------------------------
    public static List<String> getTokens(Analyzer analyzer, String field, String text)
            throws IOException {
        List<String> tokens = new ArrayList<>();
        try (TokenStream stream = analyzer.tokenStream(field, text)) {
            CharTermAttribute attr = stream.addAttribute(CharTermAttribute.class);
            stream.reset();
            while (stream.incrementToken()) {
                tokens.add(attr.toString());
            }
            stream.end();
        }
        return tokens;
    }
}