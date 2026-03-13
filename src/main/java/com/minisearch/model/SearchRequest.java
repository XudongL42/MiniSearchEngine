package com.minisearch.model;

import io.javalin.http.Context;

public class SearchRequest {
    public String query      = "*";       // query string, default match-all
    public String category   = null;      // optional category filter
    public String dateFrom   = null;      // optional date filter YYYYMMDD
    public String dateTo     = null;      // optional date filter YYYYMMDD
    public String sort       = "relevance"; // "relevance" | "date_asc" | "date_desc"
    public int    from       = 0;         // pagination offset (for searchAfter)
    public int    size       = 10;        // page size
    public boolean explain   = false;     // include score explanation

    // Parse from Javalin query params
    public static SearchRequest fromContext(Context ctx) {
        SearchRequest req = new SearchRequest();
        req.query    = ctx.queryParamAsClass("q",        String.class).getOrDefault("*");
        req.category = ctx.queryParamAsClass("category", String.class).getOrDefault(null);
        req.dateFrom = ctx.queryParamAsClass("from_date",String.class).getOrDefault(null);
        req.dateTo   = ctx.queryParamAsClass("to_date",  String.class).getOrDefault(null);
        req.sort     = ctx.queryParamAsClass("sort",     String.class).getOrDefault("relevance");
        req.from     = ctx.queryParamAsClass("from",     Integer.class).getOrDefault(0);
        req.size     = ctx.queryParamAsClass("size",     Integer.class).getOrDefault(10);
        req.explain  = ctx.queryParamAsClass("explain",  Boolean.class).getOrDefault(false);
        // Clamp size to prevent abuse
        req.size     = Math.min(req.size, 100);
        return req;
    }
}
