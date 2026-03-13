package com.minisearch.api;

import com.minisearch.IndexConfig;
import com.minisearch.model.SearchRequest;
import com.minisearch.model.SearchResponse;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.facet.*;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class SearchService implements AutoCloseable {

    private final Directory              mainDirectory;
    private final Directory              taxoDirectory;
    private final IndexWriter            writer;
    private final DirectoryTaxonomyWriter taxoWriter;
    private final SearcherManager        searcherManager;
    private final FacetsConfig           facetsConfig;
    private final Analyzer               analyzer;
    private final QueryBuilder           queryBuilder;
    private final ResultBuilder          resultBuilder;
    private final ScheduledExecutorService refresher;

    // TaxonomyReader is reopened when taxonomy changes (new categories)
    // Wrapped in AtomicReference for thread-safe swap
    private final AtomicReference<TaxonomyReader> taxoReaderRef;

    public SearchService() throws IOException {
        mainDirectory = IndexConfig.openDiskDirectory();
        taxoDirectory = IndexConfig.openTaxonomyDirectory();

        analyzer     = new EnglishAnalyzer();
        facetsConfig = new FacetsConfig();

        // Open writer in APPEND mode — preserve existing 60k documents
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        writer     = new IndexWriter(mainDirectory, config);
        taxoWriter = new DirectoryTaxonomyWriter(taxoDirectory,
                IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

        // SearcherManager: NRT search pool (from Day 6)
        searcherManager = new SearcherManager(writer, new SearcherFactory());

        // TaxonomyReader: opened from the taxonomy directory
        taxoReaderRef = new AtomicReference<>(
                new DirectoryTaxonomyReader(taxoDirectory));

        queryBuilder  = new QueryBuilder(analyzer);
        resultBuilder = new ResultBuilder(analyzer);

        // Background refresh: every 1 second, check for new documents
        // Maps directly to Elasticsearch's index.refresh_interval=1s default
        refresher = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "lucene-refresher");
            t.setDaemon(true);
            return t;
        });
        refresher.scheduleAtFixedRate(this::refresh, 1, 1, TimeUnit.SECONDS);

        System.out.printf("SearchService started. Index contains %,d documents.%n",
                writer.getDocStats().numDocs);
    }

    // ---------------------------------------------------------------
    // SEARCH: main search path
    // ---------------------------------------------------------------
    public SearchResponse search(SearchRequest req) throws Exception {
        long start = System.currentTimeMillis();

        // Build query and sort from request parameters
        QueryBuilder.BuildResult built = queryBuilder.build(req);
        Sort sort = queryBuilder.buildSort(req.sort);

        IndexSearcher searcher = searcherManager.acquire();
        try {
            // Run search + collect facets in one pass
            FacetsCollector fc      = new FacetsCollector();
            TopDocs topDocs         = runSearch(searcher, built.query(),
                    sort, req.size, fc);

            // Compute facet counts from collector
            List<FacetResult> facetResults = computeFacets(fc);

            long took = System.currentTimeMillis() - start;
            return resultBuilder.build(
                    topDocs, built.query(), searcher,
                    facetResults, built.parsedString(),
                    took, req.explain);
        } finally {
            searcherManager.release(searcher);
        }
    }

    private TopDocs runSearch(IndexSearcher searcher, Query query,
                               Sort sort, int size,
                               FacetsCollector fc) throws IOException {
        if (sort == Sort.RELEVANCE) {
            // Use FacetsCollector.search for relevance sort —
            // it handles the collector combination internally
            return FacetsCollector.search(searcher, query, size, fc);
        } else {
            // For custom sort, use TopFieldCollector + FacetsCollector together
            TopFieldCollector topCollector =
                    TopFieldCollector.create(sort, size, size);
            searcher.search(query,
                    MultiCollector.wrap(topCollector, fc));
            return topCollector.topDocs();
        }
    }

    private List<FacetResult> computeFacets(FacetsCollector fc) {
        List<FacetResult> results = new ArrayList<>();
        TaxonomyReader taxoReader = taxoReaderRef.get();
        try {
            Facets facets = new FastTaxonomyFacetCounts(
                    taxoReader, facetsConfig, fc);
            results.add(facets.getTopChildren(10, "category_facet"));
            results.add(facets.getTopChildren(5,  "author_facet"));
        } catch (Exception e) {
            // Facets are best-effort — don't fail the whole search
            System.err.println("Facet computation failed: " + e.getMessage());
        }
        return results;
    }

    // ---------------------------------------------------------------
    // INDEX: add a new document via the API
    // ---------------------------------------------------------------
    public String indexDocument(com.minisearch.model.IndexRequest req)
            throws IOException {
        String id = (req.id != null && !req.id.isBlank())
                ? req.id
                : UUID.randomUUID().toString();

        String date = (req.date != null && !req.date.isBlank())
                ? req.date
                : LocalDate.now().toString();

        int dateInt = Integer.parseInt(date.replace("-", ""));

        Document doc = new Document();
        doc.add(new StringField("id",          id,           Field.Store.YES));
        doc.add(new TextField("title",         req.title,    Field.Store.YES));
        doc.add(new TextField("description",   req.description, Field.Store.YES));
        doc.add(new StringField("category",    req.category, Field.Store.YES));
        doc.add(new StringField("author",      req.author,   Field.Store.YES));
        doc.add(new StringField("date",        date,         Field.Store.YES));
        doc.add(new IntPoint("date_int",       dateInt));
        doc.add(new StoredField("date_int_stored", dateInt));
        doc.add(new NumericDocValuesField("date_sort", dateInt));
        doc.add(new SortedDocValuesField("category_sort",
                new BytesRef(req.category)));
        doc.add(new FacetField("category_facet", req.category));
        doc.add(new FacetField("author_facet",   req.author));

        // facetsConfig.build() processes FacetField and writes to taxonomy
        writer.addDocument(facetsConfig.build(taxoWriter, doc));

        // Commit taxonomy so new categories are durable
        taxoWriter.commit();

        // Refresh taxonomy reader if new categories were added
        TaxonomyReader current = taxoReaderRef.get();
        TaxonomyReader updated = DirectoryTaxonomyReader.openIfChanged(
                (DirectoryTaxonomyReader) current);
        if (updated != null) {
            taxoReaderRef.set(updated);
            current.close();
        }

        return id;
    }

    // ---------------------------------------------------------------
    // DELETE: remove a document by id
    // ---------------------------------------------------------------
    public void deleteDocument(String id) throws IOException {
        writer.deleteDocuments(new Term("id", id));
    }

    // ---------------------------------------------------------------
    // Background refresh — called every second by the scheduler
    // ---------------------------------------------------------------
    private void refresh() {
        try {
            searcherManager.maybeRefresh();
        } catch (IOException e) {
            System.err.println("Refresh error: " + e.getMessage());
        }
    }

    // ---------------------------------------------------------------
    // Stats endpoint — useful for monitoring
    // ---------------------------------------------------------------
    public java.util.Map<String, Object> stats() throws IOException {
        IndexSearcher s = searcherManager.acquire();
        try {
            return java.util.Map.of(
                    "total_docs",    s.getIndexReader().numDocs(),
                    "deleted_docs",  s.getIndexReader().numDeletedDocs(),
                    "index_path",    IndexConfig.INDEX_PATH,
                    "taxonomy_size", taxoReaderRef.get().getSize()
            );
        } finally {
            searcherManager.release(s);
        }
    }

    @Override
    public void close() throws Exception {
        refresher.shutdown();
        searcherManager.close();
        taxoReaderRef.get().close();
        writer.commit();
        writer.close();
        taxoWriter.close();
        mainDirectory.close();
        taxoDirectory.close();
        analyzer.close();
        System.out.println("SearchService closed cleanly.");
    }
}
