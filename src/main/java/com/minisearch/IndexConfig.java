package com.minisearch;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class IndexConfig {
    // Change this path to wherever you want the index to live
    public static final String INDEX_PATH = "./data/index";

    public static final String TAXONOMY_PATH = "./data/taxonomy";

    public static Directory openDiskDirectory() throws IOException {
        Path path = Paths.get(INDEX_PATH);
        path.toFile().mkdirs(); // create folders if they don't exist
        return FSDirectory.open(path);
    }

    public static Directory openTaxonomyDirectory() throws IOException {
        Path path = Paths.get(TAXONOMY_PATH);
        path.toFile().mkdirs();
        return FSDirectory.open(path);
    }

    public static Directory openRamDirectory() {
        return new ByteBuffersDirectory();
    }
}
