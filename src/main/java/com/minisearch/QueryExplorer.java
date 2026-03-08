package com.minisearch;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.highlight.*;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.Scanner;

public class QueryExplorer {

    static DirectoryReader reader;
    static IndexSearcher   searcher;
    static Analyzer        analyzer;

    public static void main(String[] args) throws Exception {
        Directory directory = IndexConfig.openDiskDirectory();
        reader   = DirectoryReader.open(directory);
        analyzer = new EnglishAnalyzer();
        searcher = new IndexSearcher(reader);

        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.println("║          QUERY EXPLORER - DAY 4                     ║");
        System.out.println("╚══════════════════════════════════════════════════════╝");
        System.out.printf("%nIndex contains %,d documents%n%n", reader.numDocs());

        // Run all sections in order
        section1_TermQuery();
        section2_BooleanQuery();
        section3_PhraseQuery();
        section4_FuzzyAndWildcard();
        section5_RangeQuery();
        section6_QueryParser();
        section7_MultiFieldQueryParser();
        section8_InteractiveCLI();

        reader.close();
        directory.close();
        analyzer.close();
    }

    // ---------------------------------------------------------------
    // SECTION 1: TermQuery
    // The atomic building block. Matches documents containing
    // exactly one term in one field.
    // KEY RULE: TermQuery does NOT analyze its input.
    // You must pass the post-analysis form of the term.
    // ---------------------------------------------------------------
    static void section1_TermQuery() throws Exception {
        printHeader("SECTION 1: TermQuery");

        // Good: "search" is already in its analyzed form
        runSearch("TermQuery description:search",
                new TermQuery(new Term("description", "search")), 5);

        // Good: category is a StringField — no analysis, exact match
        runSearch("TermQuery category:TECH",
                new TermQuery(new Term("category", "TECH")), 5);

        // Trap: "searching" won't match because EnglishAnalyzer
        // stems it to "search" at index time. The index contains
        // "search", not "searching".
        runSearch("TermQuery description:searching (unstemmed — will miss docs)",
                new TermQuery(new Term("description", "searching")), 5);

        // Fix: use the stemmed form manually
        // Run AnalyzerPlayground to find the stem: "searching" → "search"
        runSearch("TermQuery description:search (correct stemmed form)",
                new TermQuery(new Term("description", "search")), 5);

        // Trap: wrong case on StringField
        runSearch("TermQuery category:tech (lowercase — wrong for StringField)",
                new TermQuery(new Term("category", "tech")), 5);
    }
                                
    // ---------------------------------------------------------------
    // SECTION 2: BooleanQuery
    // Combines multiple queries with MUST / SHOULD / MUST_NOT / FILTER.
    // This is what every real search query becomes internally.
    // ---------------------------------------------------------------
    static void section2_BooleanQuery() throws Exception {
        printHeader("SECTION 2: BooleanQuery");

        // MUST + MUST = AND
        // Both terms must appear. Score = sum of both sub-query scores.
        Query mustMust = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("description", "search")),  BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term("description", "news")),   BooleanClause.Occur.MUST)
                .build();
        runSearch("MUST + MUST (search AND news)", mustMust, 5);

        // MUST + SHOULD = AND with optional boost
        // Document must match the MUST clause.
        // If it also matches SHOULD, its score is higher.
        // This is how you implement "required + preferred" ranking.
        Query mustShould = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("description", "search")),  BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term("description", "trump")),   BooleanClause.Occur.SHOULD)
                .build();
        runSearch("MUST + SHOULD (search, boosted if also trump)", mustShould, 5);

        // MUST + MUST_NOT = AND NOT
        // Excludes documents matching the MUST_NOT clause entirely.
        // MUST_NOT never contributes to score.
        Query mustNot = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("description", "search")),    BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term("category", "SPORTS")),BooleanClause.Occur.MUST_NOT)
                .build();
        runSearch("MUST + MUST_NOT (search but NOT sports)", mustNot, 5);

        // MUST + FILTER = AND with no score contribution from filter
        // Use FILTER for hard constraints that shouldn't affect ranking.
        // Lucene caches FILTER clauses — faster than MUST for repeated queries.
        Query mustFilter = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("description", "search")),      BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term("category", "TECH")), BooleanClause.Occur.FILTER)
                .build();
        runSearch("MUST + FILTER (search in TECH category)", mustFilter, 5);

        // Pure SHOULD = OR
        // At least one clause must match (by default).
        // Score = sum of all matching SHOULD scores.
        Query shouldOnly = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("description", "covid")),   BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term("description", "vaccin")),    BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term("description", "pandem")), BooleanClause.Occur.SHOULD)
                .build();
        runSearch("SHOULD only (covid OR vaccine OR pandemic)", shouldOnly, 5);

        // Nested BooleanQuery — builds a query tree
        // (search OR news) AND TECH
        // This is exactly what complex ES queries compile down to.
        Query inner = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("description", "search")), BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term("description", "news")),  BooleanClause.Occur.SHOULD)
                .build();
        Query nested = new BooleanQuery.Builder()
                .add(inner, BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term("category", "TECH")),
                        BooleanClause.Occur.FILTER)
                .build();
        runSearch("Nested: (search OR news) AND category:TECH", nested, 5);
    }

    // ---------------------------------------------------------------
    // SECTION 3: PhraseQuery
    // Matches documents where terms appear in exact order at
    // consecutive positions. Uses the position data in the
    // postings list — this is why positions are stored at index time.
    //
    // Slop = how many transpositions are allowed between terms.
    // slop=0 → exact phrase
    // slop=1 → one word may appear between the terms
    // slop=N → up to N words between terms
    // ---------------------------------------------------------------
    static void section3_PhraseQuery() throws Exception {
        printHeader("SECTION 3: PhraseQuery");

        // Exact phrase — terms must be adjacent and in order
        // Remember: use analyzed (stemmed) forms
        // "white house" → analyzer → "white" "hous"
        PhraseQuery exactPhrase = new PhraseQuery.Builder()
                .add(new Term("description", "white"))
                .add(new Term("description", "hous"))
                .build();
        runSearch("PhraseQuery: \"white house\" (slop=0)", exactPhrase, 5);

        // Slop=1: one word may appear between terms
        PhraseQuery slop1 = new PhraseQuery.Builder()
                .add(new Term("description", "white"))
                .add(new Term("description", "hous"))
                .setSlop(1)
                .build();
        runSearch("PhraseQuery: \"white house\" slop=1", slop1, 5);

        // Slop=3: more permissive proximity
        PhraseQuery slop3 = new PhraseQuery.Builder()
                .add(new Term("description", "white"))
                .add(new Term("description", "hous"))
                .setSlop(3)
                .build();
        runSearch("PhraseQuery: \"white house\" slop=3", slop3, 5);

        // Three-term phrase
        // "new york city" → "new" "york" "citi"
        PhraseQuery threeTerms = new PhraseQuery.Builder()
                .add(new Term("description", "new"))
                .add(new Term("description", "york"))
                .add(new Term("description", "citi"))
                .build();
        runSearch("PhraseQuery: \"new york city\" (3 terms)", threeTerms, 5);

        // Observe: compare hit counts between slop=0, slop=1, slop=3
        // More slop = more hits = lower average precision
        // This is the precision/recall tradeoff in action
        System.out.println("  → Compare hit counts above: slop=0 < slop=1 < slop=3");
        System.out.println("    More slop = higher recall, lower precision\\n");
    }

    // ---------------------------------------------------------------
    // SECTION 4: FuzzyQuery and WildcardQuery
    //
    // FuzzyQuery: matches terms within Levenshtein edit distance N.
    // Edit distance = number of single-character insertions, deletions,
    // substitutions, or transpositions to transform one string to another.
    // "lucene" → "lusene" = edit distance 1 (one substitution)
    //
    // WildcardQuery: glob-style. * = any chars, ? = one char.
    // WARNING: both are expensive on large indexes — they iterate
    // the term dictionary. Never use on high-traffic paths without caching.
    // ---------------------------------------------------------------
    static void section4_FuzzyAndWildcard() throws Exception {
        printHeader("SECTION 4: FuzzyQuery and WildcardQuery");

        // FuzzyQuery with maxEdits=1 (default)
        // Matches "lucene", "lusene", "luceen" etc.
        // Note: FuzzyQuery operates on the RAW term, not the analyzed form.
        // It does its own internal expansion against the term dictionary.
        FuzzyQuery fuzzy1 = new FuzzyQuery(new Term("title", "lucene"), 1);
        runSearch("FuzzyQuery title:lucene~1 (edit distance 1)", fuzzy1, 5);

        // FuzzyQuery with maxEdits=2
        // Matches more variants — also more false positives
        FuzzyQuery fuzzy2 = new FuzzyQuery(new Term("title", "lucene"), 2);
        runSearch("FuzzyQuery title:lucene~2 (edit distance 2)", fuzzy2, 5);

        // Typo tolerance demo: deliberate misspellings
        FuzzyQuery typo1 = new FuzzyQuery(new Term("title", "serach"), 2);
        // Rewrite to see the expanded BooleanQuery
        //Query rewritten = typo1.rewrite(reader);
        //System.out.println("FuzzyQuery expands to: " + rewritten);
        runSearch("FuzzyQuery title:serach~2 (typo: 'serach' → 'search')", typo1, 5);

        FuzzyQuery typo2 = new FuzzyQuery(new Term("title", "elsticsearch"), 2);
        runSearch("FuzzyQuery title:elsticsearch~2 (typo)", typo2, 5);

        // PrefixQuery: terms starting with a prefix
        // More efficient than WildcardQuery for prefix-only patterns
        PrefixQuery prefix = new PrefixQuery(new Term("title", "search"));
        runSearch("PrefixQuery title:search* (prefix)", prefix, 5);

        // WildcardQuery: ? = exactly one char, * = zero or more chars
        // "s?arch" matches "search", "starch" but not "srch"
        WildcardQuery wildcard1 = new WildcardQuery(new Term("title", "s?arch*"));
        runSearch("WildcardQuery title:s?arch* ", wildcard1, 5);

        // Leading wildcard — very expensive (full term dictionary scan)
        // Never use in production without wrapping in a cache
        WildcardQuery wildcard2 = new WildcardQuery(new Term("title", "*search*"));
        runSearch("WildcardQuery title:*search* (leading wildcard — slow!)", wildcard2, 5);

        System.out.println("  → Notice: FuzzyQuery and WildcardQuery both expand");
        System.out.println("    to multiple TermQueries internally (rewrite phase).");
        System.out.println("    Use sparingly on large indexes.\\n");
    }

    // ---------------------------------------------------------------
    // SECTION 5: Range Queries
    // IntPoint.newRangeQuery for numeric fields.
    // TermRangeQuery for string/lexicographic ranges.
    //
    // IMPORTANT: You cannot use TermQuery or RangeQuery on IntPoint
    // fields. IntPoint has its own query factory methods.
    // ---------------------------------------------------------------
    static void section5_RangeQuery() throws Exception {
        printHeader("SECTION 5: Range Queries");

        // Numeric range on IntPoint field
        // date_int format: YYYYMMDD
        Query range2024 = IntPoint.newRangeQuery("date_int", 20240101, 20241231);
        runSearch("IntPoint date_int:[20240101 TO 20241231] (all of 2024)", range2024, 5);

        Query range2022to2023 = IntPoint.newRangeQuery("date_int", 20220101, 20231231);
        runSearch("IntPoint date_int:[20220101 TO 20231231] (2022-2023)", range2022to2023, 5);

        // Open-ended range: 2023 and later
        // Integer.MAX_VALUE as upper bound = no upper limit
        Query from2023 = IntPoint.newRangeQuery("date_int", 20230101, Integer.MAX_VALUE);
        runSearch("IntPoint date_int:[20230101 TO *] (2023 onwards)", from2023, 5);

        // Exact value match via range query
        // IntPoint.newExactQuery is syntactic sugar for newRangeQuery(f, v, v)
        Query exactDate = IntPoint.newExactQuery("date_int", 20240315);
        runSearch("IntPoint exact: date_int=20240315", exactDate, 5);

        // TermRangeQuery on string fields — lexicographic order
        // Matches categories alphabetically between HEALTH and SPORTS
        Query termRange = TermRangeQuery.newStringRange(
                "category", "HEALTH", "SPORTS", true, true);
        runSearch("TermRangeQuery category:[HEALTH TO SPORTS] (lexicographic)", termRange, 5);

        // Combine text search with date filter — the core production pattern
        Query textPlusDate = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("description", "search")),         BooleanClause.Occur.MUST)
                .add(IntPoint.newRangeQuery("date_int", 20230101, 20241231),
                        BooleanClause.Occur.FILTER)
                .build();
        runSearch("description:search AND date:[2023 TO 2024]", textPlusDate, 5);
    }

    // ---------------------------------------------------------------
    // SECTION 6: QueryParser
    // Parses a human-typed string into a query tree.
    // Applies your analyzer to text terms automatically.
    // Supports a rich mini-language that users can type directly.
    //
    // QueryParser syntax reference:
    // https://lucene.apache.org/core/9_12_0/queryparser/org/apache/lucene/queryparser/classic/package-summary.html
    // ---------------------------------------------------------------
    static void section6_QueryParser() throws Exception {
        printHeader("SECTION 6: QueryParser");

        QueryParser parser = new QueryParser("description", analyzer);

        // Basic term — analyzer runs on "searching"
        // "searching" → stems to "search" → TermQuery(description, search)
        parseAndSearch(parser, "searching",
                "single term (auto-analyzed)");

        // Explicit AND
        parseAndSearch(parser, "search AND index",
                "explicit AND");

        // Explicit OR
        parseAndSearch(parser, "lucene OR solr",
                "explicit OR");

        // Default OR (implicit between terms)
        parseAndSearch(parser, "lucene search index",
                "implicit OR between terms");

        // Switch default to AND — more restrictive, higher precision
        parser.setDefaultOperator(QueryParser.Operator.AND);
        parseAndSearch(parser, "lucene search index",
                "same query but defaultOperator=AND");
        parser.setDefaultOperator(QueryParser.Operator.OR); // reset

        // Phrase query via quotes
        parseAndSearch(parser, "\"search engine\"",
                "phrase query via quotes");

        // Phrase with slop
        parseAndSearch(parser, "\"search engine\"~2",
                "phrase with slop=2");

        // Fuzzy via tilde
        parseAndSearch(parser, "lucene~1",
                "fuzzy via tilde");

        // Field-specific search
        parseAndSearch(parser, "title:house description:search",
                "field-specific terms");

        // Boosting a term (^ operator) — preview of Day 5
        parseAndSearch(parser, "title:house^3 description:search",
                "title boosted 3x over description");

        // Wildcard
        parseAndSearch(parser, "title:search*",
                "wildcard in QueryParser");

        // NOT operator
        parseAndSearch(parser, "search NOT sports",
                "NOT sports");

        // Complex real-world query
        parseAndSearch(parser, "title:house OR (description:search AND description:index)",
                "complex nested query");

        // Special characters that need escaping
        // QueryParser treats + - && || ! ( ) { } [ ] ^ " ~ * ? : \\ /  as operators
        // Escape with backslash to treat as literals
        parseAndSearch(parser, "user\\\\@example\\\\.com",
                "escaped special chars");
    }

    // ---------------------------------------------------------------
    // SECTION 7: MultiFieldQueryParser
    // Like QueryParser but searches across multiple fields at once.
    // Each field can have a different boost weight.
    // This is the closest equivalent to ES's multi_match query.
    // ---------------------------------------------------------------
    static void section7_MultiFieldQueryParser() throws Exception {
        printHeader("SECTION 7: MultiFieldQueryParser");

        // Search title and description with equal weight
        String[] fields  = {"title", "description"};
        float[]  boosts  = {1.0f,    1.0f};

        MultiFieldQueryParser multiParser = new MultiFieldQueryParser(
                fields, analyzer,
                buildBoostMap(fields, boosts));

        parseAndSearchMulti(multiParser, "search engine",
                "title + body equal weight");

        // Title boosted 3x — title matches rank higher
        float[] boostedWeights = {3.0f, 1.0f};
        MultiFieldQueryParser boostedParser = new MultiFieldQueryParser(
                fields, analyzer,
                buildBoostMap(fields, boostedWeights));

        parseAndSearchMulti(boostedParser, "search engine",
                "title boosted 3x");

        // Compare: same query, different boost ratios
        // Watch how result order changes
        parseAndSearchMulti(boostedParser, "lucene index",
                "lucene + index with title boost");

        // Three fields: title, description, category
        String[] threeFields  = {"title", "description", "category"};
        float[]  threeBoosts  = {3.0f,    1.0f,   2.0f};
        MultiFieldQueryParser threeParser = new MultiFieldQueryParser(
                threeFields, analyzer,
                buildBoostMap(threeFields, threeBoosts));

        parseAndSearchMulti(threeParser, "technology search",
                "title(3x) + description(1x) + category(2x)");
    }

    // ---------------------------------------------------------------
    // SECTION 8: Interactive CLI
    // Type any QueryParser query string and see live results.
    // This lets you explore the index freely beyond the scripted tests.
    // ---------------------------------------------------------------
    static void section8_InteractiveCLI() throws Exception {
        printHeader("SECTION 8: Interactive CLI");

        System.out.println("QueryParser is configured with:");
        System.out.println("  Default field: description");
        System.out.println("  Analyzer:      EnglishAnalyzer");
        System.out.println("  Default op:    OR");
        System.out.println();
        System.out.println("Syntax hints:");
        System.out.println("  term              → description:term (analyzed)");
        System.out.println("  title:term        → specific field");
        System.out.println("  \"exact phrase\"    → phrase query");
        System.out.println("  term~1            → fuzzy");
        System.out.println("  term*             → wildcard/prefix");
        System.out.println("  a AND b           → must match both");
        System.out.println("  a OR b            → either");
        System.out.println("  +a -b             → must have a, must not have b");
        System.out.println("  title:a^3 description:b  → field boost");
        System.out.println();
        System.out.println("Type 'quit' to exit.\\n");

        QueryParser parser = new QueryParser("description", analyzer);
        parser.setAllowLeadingWildcard(true);
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.print("lucene> ");
            String input = scanner.nextLine().trim();

            if (input.equalsIgnoreCase("quit") || input.equalsIgnoreCase("exit")) {
                System.out.println("Exiting interactive mode.");
                break;
            }
            if (input.isEmpty()) continue;

            try {
                Query query = parser.parse(input);
                System.out.println("  Parsed query: " + query.toString());
                runSearch("→ " + input, query, 10);
            } catch (ParseException e) {
                System.out.println("  Parse error: " + e.getMessage());
                System.out.println("  Try escaping special characters with \\\\");
            }
        }
    }

    // ---------------------------------------------------------------
    // HELPERS
    // ---------------------------------------------------------------

    static void runSearch(String label, Query query, int topN) throws Exception {
        TopDocs results = searcher.search(query, topN);
        long total = results.totalHits.value;

        System.out.printf("  %-55s → %,d hits%n", label, total);

        // Set up highlighter to show matching snippets
        QueryScorer scorer = new QueryScorer(query);
        Highlighter highlighter = new Highlighter(scorer);
        highlighter.setTextFragmenter(new SimpleFragmenter(60));

        for (ScoreDoc sd : results.scoreDocs) {
            Document doc   = searcher.doc(sd.doc);
            String title   = doc.get("title");
            String description = doc.get("description") != null ? doc.get("description") : "";
            String category = doc.get("category");
            String date    = doc.get("date");

            // Get highlighted snippet from description
            String snippet = null;
            try {
                org.apache.lucene.analysis.TokenStream ts =
                        analyzer.tokenStream("description", description);
                snippet = highlighter.getBestFragment(ts, description);
            } catch (Exception ignored) {}

            System.out.printf("    [%.3f] %-8s %s | %s%n",
                    sd.score, category, date, title);
            if (snippet != null) {
                System.out.printf("           snippet: ...%s...%n", snippet);
            }
        }
        if (results.scoreDocs.length > 0) System.out.println();
    }

    static void parseAndSearch(QueryParser parser, String queryStr,
                               String label) throws Exception {
        try {
            Query query = parser.parse(queryStr);
            System.out.printf("  Input:   \"%s\" [%s]%n", queryStr, label);
            System.out.printf("  Parsed:  %s%n", query.toString());
            TopDocs results = searcher.search(query, 3);
            System.out.printf("  Hits:    %,d%n", results.totalHits.value);
            for (ScoreDoc sd : results.scoreDocs) {
                Document doc = searcher.doc(sd.doc);
                System.out.printf("    [%.3f] %s%n", sd.score, doc.get("title"));
            }
            System.out.println();
        } catch (ParseException e) {
            System.out.printf("  Input:  \"%s\" → PARSE ERROR: %s%n%n",
                    queryStr, e.getMessage());
        }
    }

    static void parseAndSearchMulti(MultiFieldQueryParser parser,
                                    String queryStr, String label) throws Exception {
        try {
            Query query = parser.parse(queryStr);
            System.out.printf("  Input:   \"%s\" [%s]%n", queryStr, label);
            System.out.printf("  Parsed:  %s%n", query.toString());
            TopDocs results = searcher.search(query, 5);
            System.out.printf("  Hits:    %,d%n", results.totalHits.value);
            for (ScoreDoc sd : results.scoreDocs) {
                Document doc = searcher.doc(sd.doc);
                System.out.printf("    [%.3f] %s%n", sd.score, doc.get("title"));
            }
            System.out.println();
        } catch (ParseException e) {
            System.out.printf("  Input:  \"%s\" → PARSE ERROR: %s%n%n",
                    queryStr, e.getMessage());
        }
    }

    static java.util.Map<String, Float> buildBoostMap(String[] fields, float[] boosts) {
        java.util.Map<String, Float> map = new java.util.HashMap<>();
        for (int i = 0; i < fields.length; i++) {
            map.put(fields[i], boosts[i]);
        }
        return map;
    }

    static void printHeader(String title) {
        System.out.println("\\n━━━ " + title + " ━━━\\n");
    }
}
