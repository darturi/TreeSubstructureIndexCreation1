import org.neo4j.driver.*;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.ProfiledPlan;
import java.util.concurrent.TimeUnit;

import java.util.*;

/**
 * Drop this file next to Neo4jDriver.java and TreeGenerator.java.
 *
 * Usage ideas:
 *   - Run directly with a main() to compare two queries.
 *   - Call QueryBenchmark.compare(...) from your own code/tests.
 */
public class QueryBenchmark implements AutoCloseable {

    private final Driver driver;

    public QueryBenchmark(String uri, String user, String password) {
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    @Override
    public void close() {
        driver.close();
    }

    /** One execution result with collected metrics. */
    public static final class Sample {
        public final double wallMs;
        public final long availableMs;  // -1 if unknown
        public final long consumedMs;   // -1 if unknown
        public final Long dbHits;       // null if not profiled
        public final Long rows;         // null if not profiled

        Sample(double wallMs, long availableMs, long consumedMs, Long dbHits, Long rows) {
            this.wallMs = wallMs;
            this.availableMs = availableMs;
            this.consumedMs = consumedMs;
            this.dbHits = dbHits;
            this.rows = rows;
        }
    }

    /** Aggregate statistics for one query. */
    public static final class Stats {
        public final Double wallMs;
        public final Double availableMs;
        public final Double consumedMs;
        public final Double dbHits;
        public final Double rows;

        Stats(Double wallMs, Double availableMs, Double consumedMs, Double dbHits, Double rows) {
            this.wallMs = wallMs;
            this.availableMs = availableMs;
            this.consumedMs = consumedMs;
            this.dbHits = dbHits;
            this.rows = rows;
        }
    }

    /** Comparison result across both queries. */
    public static final class ComparisonResult {
        public final Stats A;
        public final Stats B;
        public final Stats deltaBminusA;
        public final int runs;
        public final int warmups;

        ComparisonResult(Stats a, Stats b, Stats delta, int runs, int warmups) {
            this.A = a;
            this.B = b;
            this.deltaBminusA = delta;
            this.runs = runs;
            this.warmups = warmups;
        }
    }

    private static String ensureProfiled(String query, boolean useProfile) {
        if (!useProfile) return query;
        String q = query.stripLeading();
        String upper = q.toUpperCase(Locale.ROOT);
        if (upper.startsWith("PROFILE") || upper.startsWith("EXPLAIN")) return q;
        return "PROFILE " + q;
    }

    private Sample runOnce(Session session, String query, Map<String,Object> params, boolean useProfile) {
        String q = ensureProfiled(query, useProfile);
        long t0 = System.nanoTime();
        Result result = (params == null || params.isEmpty())
                ? session.run(q)
                : session.run(q, params);
        // fully consume
        ResultSummary summary = result.consume();
        long t1 = System.nanoTime();

        double wallMs = (t1 - t0) / 1_000_000.0;
        long availableMs = summary.resultAvailableAfter(TimeUnit.MILLISECONDS);
        long consumedMs  = summary.resultConsumedAfter(TimeUnit.MILLISECONDS);

        // Pull simple profile totals (if present)
        Long dbHits = null;
        Long rows   = null;
        ProfiledPlan prof = summary.profile();
        if (prof != null) {
            // Root node often has aggregated totals; fall back to a recursive sum if needed.
            dbHits = safeLong(prof.dbHits());
            rows   = safeLong(prof.records());
        }
        return new Sample(wallMs, availableMs, consumedMs, dbHits, rows);
    }

    private static Long safeLong(long v) {
        return v >= 0 ? v : null;
    }

    private static Double avgDouble(Collection<Double> vals) {
        if (vals.isEmpty()) return null;
        double s = 0; int n = 0;
        for (Double d : vals) if (d != null) { s += d; n++; }
        return n == 0 ? null : (s / n);
    }

    private static Double avgLongs(Collection<Long> vals) {
        if (vals.isEmpty()) return null;
        long s = 0; int n = 0;
        for (Long d : vals) if (d != null) { s += d; n++; }
        return n == 0 ? null : (s * 1.0 / n);
    }

    private static String fmtMs(Double v)   { return v == null ? "—" : String.format(Locale.ROOT, "%.2f ms", v); }
    private static String fmtInt(Double v)  { return v == null ? "—" : String.format(Locale.ROOT, "%,.0f", v); }
    private static String pad(String s, int w) { return s + " ".repeat(Math.max(0, w - s.length())); }

    private static Stats summarize(List<Sample> samples) {
        List<Double> wall   = new ArrayList<>();
        List<Double> avail  = new ArrayList<>();
        List<Double> cons   = new ArrayList<>();
        List<Long>   hits   = new ArrayList<>();
        List<Long>   rows   = new ArrayList<>();

        for (Sample s : samples) {
            wall.add(s.wallMs);
            avail.add((double) (s.availableMs >= 0 ? s.availableMs : Double.NaN));
            cons.add((double) (s.consumedMs >= 0 ? s.consumedMs : Double.NaN));
            if (s.dbHits != null) hits.add(s.dbHits);
            if (s.rows   != null) rows.add(s.rows);
        }
        // Filter out NaNs we introduced from missing timings
        avail.removeIf(d -> d.isNaN());
        cons.removeIf(d -> d.isNaN());

        return new Stats(
                avgDouble(wall),
                avgDouble(avail),
                avgDouble(cons),
                avgLongs(hits),
                avgLongs(rows)
        );
    }

    private static Stats delta(Stats b, Stats a) {
        Double dWall = (a.wallMs != null && b.wallMs != null) ? (b.wallMs - a.wallMs) : null;
        Double dAvail = (a.availableMs != null && b.availableMs != null) ? (b.availableMs - a.availableMs) : null;
        Double dCons  = (a.consumedMs != null && b.consumedMs != null) ? (b.consumedMs - a.consumedMs) : null;
        Double dHits  = (a.dbHits != null && b.dbHits != null) ? (b.dbHits - a.dbHits) : null;
        Double dRows  = (a.rows != null && b.rows != null) ? (b.rows - a.rows) : null;
        return new Stats(dWall, dAvail, dCons, dHits, dRows);
    }

    /**
     * Compare two queries over multiple runs.
     *
     * @param queryA   Cypher query A
     * @param queryB   Cypher query B
     * @param paramsA  parameters (nullable)
     * @param paramsB  parameters (nullable)
     * @param runs     measured runs per query (excludes warmups)
     * @param warmups  warm-up executions per query (not measured)
     * @param shuffle  alternate execution order to reduce cache bias
     * @param useProfile add PROFILE unless already present
     */
    public ComparisonResult compare(
            String queryA, String queryB,
            Map<String,Object> paramsA, Map<String,Object> paramsB,
            int runs, int warmups, boolean shuffle, boolean useProfile
    ) {
        List<Sample> A = new ArrayList<>();
        List<Sample> B = new ArrayList<>();

        try (Session session = driver.session(SessionConfig.defaultConfig())) {

            // Warmups
            for (int i = 0; i < warmups; i++) {
                runOnce(session, queryA, paramsA, useProfile);
                runOnce(session, queryB, paramsB, useProfile);
            }

            // Measured runs
            for (int i = 0; i < runs; i++) {
                boolean abFirst = shuffle ? (i % 2 == 0) : true;
                if (abFirst) {
                    A.add(runOnce(session, queryA, paramsA, useProfile));
                    B.add(runOnce(session, queryB, paramsB, useProfile));
                } else {
                    B.add(runOnce(session, queryB, paramsB, useProfile));
                    A.add(runOnce(session, queryA, paramsA, useProfile));
                }
            }
        }

        Stats sA = summarize(A);
        Stats sB = summarize(B);
        Stats d  = delta(sB, sA);

        printTable(sA, sB, d);

        return new ComparisonResult(sA, sB, d, runs, warmups);
    }

    private void printTable(Stats a, Stats b, Stats d) {
        String[] header = {"Metric", "Query A (avg)", "Query B (avg)", "Δ (B - A)"};
        String[][] rows = new String[][]{
                {"Wall time",        fmtMs(a.wallMs),      fmtMs(b.wallMs),      fmtMs(d.wallMs)},
                {"Result available", fmtMs(a.availableMs), fmtMs(b.availableMs), fmtMs(d.availableMs)},
                {"Result consumed",  fmtMs(a.consumedMs),  fmtMs(b.consumedMs),  fmtMs(d.consumedMs)},
                {"DB hits",          fmtInt(a.dbHits),     fmtInt(b.dbHits),     fmtInt(d.dbHits)},
                {"Rows",             fmtInt(a.rows),       fmtInt(b.rows),       fmtInt(d.rows)}
        };
        int[] w = new int[]{header[0].length(), header[1].length(), header[2].length(), header[3].length()};
        for (String[] r : rows) for (int i = 0; i < r.length; i++) w[i] = Math.max(w[i], r[i].length());

        StringBuilder out = new StringBuilder();
        for (int i = 0; i < header.length; i++) {
            out.append(pad(header[i], w[i])).append(i < header.length - 1 ? "  " : "");
        }
        out.append('\n').append("-".repeat(Arrays.stream(w).sum() + 2 * (w.length - 1))).append('\n');
        for (String[] r : rows) {
            for (int i = 0; i < r.length; i++) {
                out.append(pad(r[i], w[i])).append(i < r.length - 1 ? "  " : "");
            }
            out.append('\n');
        }
        System.out.print(out.toString());
    }

    // --- Optional demo main() you can run directly ---
    public static void main(String[] args) {
        String uri = "neo4j://127.0.0.1:7687";
        String user = "neo4j";
        String password = "gigglebox"; // <-- change to match your instance

        // Example queries that should be semantically equivalent
        String query1 = """
                PROFILE MATCH (root:TypeC {name: "A"})
                MATCH (root)-[*0..]->(descendant)
                RETURN descendant;
                                
                """;
        String query2 = """
                PROFILE MATCH (a:TypeC {name: "A"})
                MATCH (c:TypeC)
                WHERE c.new_id_label >= a.new_id_label\s
                  AND c.new_id_label < a.new_id_label + a.interval_width
                RETURN c;
                """;

        Map<String,Object> params = new HashMap<>();
        params.put("name", "Acme");

        // (Optional) Use your existing seeder before benchmarking
        try (Neo4jDriver app = new Neo4jDriver(uri, user, password)) {
            app.testConnection();
            // Choose 1 or 2 depending on which tree you want:
            app.createSampleData(2);
        } catch (Exception e) {
            System.err.println("Seeding failed (continuing anyway): " + e.getMessage());
        }

        try (QueryBenchmark bench = new QueryBenchmark(uri, user, password)) {
            bench.compare(
                    query1, query2,
                    params, params,
                    /* runs   */ 7,
                    /* warmups*/ 2,
                    /* shuffle*/ true,
                    /* profile*/ true
            );
        }
    }
}
