package com.minisearch.model;

public class IndexRequest {
    public String id;           // optional, generated if absent
    public String title;
    public String description;
    public String category  = "GENERAL";
    public String author    = "unknown";
    public String date;         // YYYY-MM-DD, defaults to today

    public String validate() {
        if (title == null || title.isBlank())
            return "title is required";
        if (description == null || description.isBlank())
            return "description is required";
        return null; // null = valid
    }
}
