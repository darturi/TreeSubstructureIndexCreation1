import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.summary.ProfiledPlan;
import org.neo4j.driver.summary.ResultSummary;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Neo4jProfiler {

    public static void runProfileQuery(Driver driver, String cypher, Map<String, Object> parameters, boolean clearCache, boolean verboseClearCache) {
        try (Session session = driver.session()) {

            // Clear cache if requested
            if (clearCache) {
                clearNeo4jCache(session, verboseClearCache);
            }

            // Add PROFILE keyword to the beginning of the query if not already present
            String profileQuery = cypher.trim().toUpperCase().startsWith("PROFILE") ?
                    cypher : "PROFILE " + cypher;

            System.out.println("Executing profiled query: " + profileQuery);
            System.out.println("Parameters: " + parameters);
            if (clearCache) {
                System.out.println("Cache cleared before execution");
            }
            System.out.println("=" + "=".repeat(80));

            // Execute the profiled query
            Result result = session.run(profileQuery, parameters);

            // Display query results
            System.out.println("QUERY RESULTS:");
            System.out.println("-".repeat(40));
            int recordCount = 0;
            while (result.hasNext()) {
                Record record = result.next();
                System.out.println("Record " + (++recordCount) + ": " + record.asMap());
            }

            if (recordCount == 0) {
                System.out.println("No records returned");
            }

            // Get and display profiling information
            ResultSummary summary = result.consume();
            displayProfilingResults(summary);

        } catch (Exception e) {
            System.err.println("Error executing profiled query: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Clears Neo4j's page cache to ensure cold runs for performance testing
     */
    private static void clearNeo4jCache(Session session, boolean verbose) {
        // Try different cache clearing procedures based on Neo4j version

        // 1. Clear query plan cache (most commonly available)
        try {
            session.run("CALL db.clearQueryCaches()");
            if (verbose) System.out.println("Query caches cleared");
        } catch (Exception e) {
            System.out.println("Query cache clear not available: " + e.getMessage());
        }

        // 2. Try alternative query cache clearing (older versions)
        try {
            session.run("CALL dbms.clearQueryCaches()");
            if (verbose) System.out.println("Query caches cleared (dbms version)");
        } catch (Exception e) {
            // Silently continue - this is expected if the procedure doesn't exist
        }

        // 3. Clear page cache (most important for performance testing)
        try {
            session.run("CALL db.clearPageCache()");
            if (verbose) System.out.println("Page cache cleared");
        } catch (Exception e) {
            try {
                // Try alternative page cache clearing
                session.run("CALL dbms.clearPageCache()");
                System.out.println("Page cache cleared (dbms version)");
            } catch (Exception e2) {
                System.out.println("Page cache clear not available (requires admin privileges)");
            }
        }

        // 4. Manual memory pressure approach (fallback)
        try {
            // Force garbage collection to free up memory
            System.gc();
            if (verbose) System.out.println("Forced garbage collection");

            // Small delay to let GC complete
            Thread.sleep(50);
        } catch (Exception e) {
            // Continue silently
        }
    }

    public static double sumTimeValues(String s) {
        if (s == null || s.isEmpty()) {
            return 0.0;
        }

        // Regular expression to match "Time={number}," pattern
        // This handles integers, decimals, and negative numbers
        Pattern pattern = Pattern.compile("Time=([+-]?\\d+(?:\\.\\d+)?),");
        Matcher matcher = pattern.matcher(s);

        double sum = 0.0;

        // Find all matches and sum the numbers
        while (matcher.find()) {
            String numberStr = matcher.group(1); // Extract the number from group 1
            sum += Double.parseDouble(numberStr);
        }

        return sum;
    }

    /**
     * Runs multiple iterations of a query with cache clearing for performance benchmarking
     * Note: Runtime that is collected is based on a start and stop timer in the java,
     * rather than pulling directly from profile results
     */
    public static void benchmarkQuery(Driver driver, String cypher, Map<String, Object> parameters, int iterations, boolean verbose) {
        System.out.println("Benchmarking query over " + iterations + " iterations with cache clearing:");
        System.out.println("Query: " + cypher);
        System.out.println("=" + "=".repeat(100));

        long totalDbHits = 0;
        long totalRecords = 0;
        double totalTime = 0.0;

        for (int i = 1; i <= iterations; i++) {
            if (verbose) {
                System.out.println("\n--- Iteration " + i + " ---");
            }

            try (Session session = driver.session()) {
                // Clear cache before each run
                clearNeo4jCache(session, false);

                // Small delay to ensure cache clearing takes effect
                Thread.sleep(100);

                // Add PROFILE keyword
                String profileQuery = cypher.trim().toUpperCase().startsWith("PROFILE") ?
                        cypher : "PROFILE " + cypher;

                // Execute query
                long startTime = System.nanoTime();

                Result result;
                if (parameters != null) {
                    result = session.run(profileQuery, parameters);
                } else {
                    result = session.run(profileQuery);
                }

                // Consume all results
                int recordCount = 0;
                while (result.hasNext()) {
                    result.next();
                    recordCount++;
                }

                long endTime = System.nanoTime();
                double executionTimeMs = (endTime - startTime) / 1_000_000.0;

                // Get profiling info
                ResultSummary summary = result.consume();
                System.out.println(summary.toString());

                //System.out.println("111NOTICE!!!!");
                //System.out.println(sumTimeValues(summary.toString()));
                //System.out.println("222NOTICE!!!!");

                long dbHits = summary.profile().dbHits();

                totalDbHits += dbHits;
                totalRecords += recordCount;
                totalTime += executionTimeMs;

                if (verbose) {
                    System.out.println("  DB Hits: " + dbHits);
                    System.out.println("  Records: " + recordCount);
                    System.out.println("  Execution Time: " + String.format("%.3f", executionTimeMs) + " ms");
                }

            } catch (Exception e) {
                System.err.println("Error in iteration " + i + ": " + e.getMessage());
            }
        }

        // Display averages
        System.out.println("\n" + "=".repeat(50));
        System.out.println("BENCHMARK RESULTS:");
        System.out.println("=".repeat(50));
        System.out.println("Iterations: " + iterations);
        System.out.println("Average DB Hits: " + (totalDbHits / iterations));
        System.out.println("Average Records: " + (totalRecords / iterations));
        System.out.println("Average Execution Time: " + String.format("%.3f", totalTime / iterations) + " ms");
        System.out.println("Total Time: " + String.format("%.3f", totalTime) + " ms");
    }

    private static void displayProfilingResults(ResultSummary summary) {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("PROFILING RESULTS:");
        System.out.println("=".repeat(80));

        // Basic query statistics
        System.out.println("Query Type: " + summary.queryType());
        System.out.println("Database Hits: " + summary.profile().dbHits());
        System.out.println("Records: " + summary.profile().records());

        // Note: Execution time is included in the PROFILE results below

        // Counters (for write operations)
        if (summary.counters().containsUpdates()) {
            System.out.println("\nUpdate Counters:");
            System.out.println("  Nodes Created: " + summary.counters().nodesCreated());
            System.out.println("  Nodes Deleted: " + summary.counters().nodesDeleted());
            System.out.println("  Relationships Created: " + summary.counters().relationshipsCreated());
            System.out.println("  Relationships Deleted: " + summary.counters().relationshipsDeleted());
            System.out.println("  Properties Set: " + summary.counters().propertiesSet());
            System.out.println("  Labels Added: " + summary.counters().labelsAdded());
            System.out.println("  Labels Removed: " + summary.counters().labelsRemoved());
        }

        // Detailed execution plan
        ProfiledPlan plan = summary.profile();
        if (plan != null) {
            System.out.println("\nEXECUTION PLAN:");
            System.out.println("-".repeat(50));
            displayExecutionPlan(plan, 0);
        }
    }

    private static void displayExecutionPlan(ProfiledPlan plan, int depth) {
        String indent = "  ".repeat(depth);

        System.out.println(indent + "Operator: " + plan.operatorType());

        // Display arguments/details
        Map<String, Value> arguments = plan.arguments();
        if (!arguments.isEmpty()) {
            System.out.println(indent + "Arguments: " + arguments);
        }

        System.out.println(indent + "DB Hits: " + plan.dbHits());
        System.out.println(indent + "Records: " + plan.records());

        if (plan.time() > 0) {
            // Time is typically returned in microseconds, convert to milliseconds
            double timeInMs = plan.time() / 1000.0;
            System.out.println(indent + "Time: " + String.format("%.3f", timeInMs) + " ms");
        }

        // Display child plans recursively
        for (ProfiledPlan child : plan.children()) {
            System.out.println(indent + "├─ Child Plan:");
            displayExecutionPlan(child, depth + 1);
        }

        if (depth == 0) {
            System.out.println();
        }
    }

    // Example usage method
    public static void main(String[] args) {
        // Example connection setup
        String uri = "neo4j://127.0.0.1:7687";
        String username = "neo4j";
        String password = "gigglebox";

        try (Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password))) {

            // Example 1: Simple node query with cache clearing
            //System.out.println("Example 1: Simple Node Query (Cold Run)");
            //runProfileQuery(driver,
            //        "MATCH (n:Person) RETURN n.name, n.age LIMIT 10",
            //        Map.of(), true);

            //System.out.println("\n" + "=".repeat(100) + "\n");

            // Example 2: Same query without cache clearing (warm run)
            //System.out.println("Example 2: Same Query (Warm Run)");
            //runProfileQuery(driver,
            //        "MATCH (n:Person) RETURN n.name, n.age LIMIT 10",
            //        Map.of(), false);

            System.out.println("\n" + "=".repeat(100) + "\n");

            // Example 3: Benchmark a query over multiple iterations
            System.out.println("Example 3: Benchmark Query");
            benchmarkQuery(driver,
                    """
                            PROFILE MATCH (a:TypeC {name: "A"})
                            MATCH (c:TypeC)
                            WHERE c.new_id_label >= a.new_id_label\s
                              AND c.new_id_label < a.new_id_label + a.interval_width
                            RETURN c;""",
                    null,
                    5,
                    false
                    );

        } catch (Exception e) {
            System.err.println("Database connection error: " + e.getMessage());
        }
    }
}