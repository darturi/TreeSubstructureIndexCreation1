import org.neo4j.driver.*;
import org.neo4j.driver.summary.ProfiledPlan;
import org.neo4j.driver.summary.ResultSummary;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class CypherQueryComparator {

    private final Driver driver;

    public CypherQueryComparator(String uri, String username, String password) {
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password));
    }

    public static class QueryMetrics {
        private final String queryName;
        private final List<Long> executionTimes = new ArrayList<>();
        private final List<Long> dbHits = new ArrayList<>();
        private final List<Long> resultRows = new ArrayList<>();
        private final List<Long> memoryUsage = new ArrayList<>();

        public QueryMetrics(String queryName) {
            this.queryName = queryName;
        }

        public QueryStats getStatistics() {
            return new QueryStats(queryName, executionTimes, dbHits, resultRows, memoryUsage);
        }

        public long getTotalDbHits(ProfiledPlan plan) {
            long totalHits = plan.dbHits();
            // Recursively sum db hits from child plans
            for (ProfiledPlan child : plan.children()) {
                totalHits += getTotalDbHits(child);
            }
            return totalHits;
        }

        public long extractMemoryUsage(ProfiledPlan plan) {
            // Try to extract memory information from plan arguments
            Map<String, Value> args = plan.arguments();

            // Different Neo4j versions might have different keys
            Object memory = args.get("Memory");
            if (memory == null) {
                memory = args.get("EstimatedRows");
            }
            if (memory == null) {
                memory = args.get("PageCacheHits");
            }

            if (memory instanceof Number) {
                return ((Number) memory).longValue();
            }

            // Recursively check child plans
            long totalMemory = 0;
            for (ProfiledPlan child : plan.children()) {
                totalMemory += extractMemoryUsage(child);
            }

            return totalMemory;
        }
    }

    public static class QueryStats {
        private final String queryName;
        private final double avgExecutionTime;
        private final double minExecutionTime;
        private final double maxExecutionTime;
        private final double stdDevExecutionTime;
        private final double avgDbHits;
        private final double avgResultRows;
        private final double avgMemoryUsage;
        private final int runCount;

        public QueryStats(String queryName, List<Long> execTimes, List<Long> dbHits,
                          List<Long> resultRows, List<Long> memoryUsage) {
            this.queryName = queryName;
            this.runCount = execTimes.size();

            // Calculate execution time statistics
            this.avgExecutionTime = execTimes.stream().mapToLong(Long::longValue).average().orElse(0.0);
            this.minExecutionTime = execTimes.stream().mapToLong(Long::longValue).min().orElse(0);
            this.maxExecutionTime = execTimes.stream().mapToLong(Long::longValue).max().orElse(0);

            // Calculate standard deviation
            double variance = execTimes.stream()
                    .mapToDouble(time -> Math.pow(time - avgExecutionTime, 2))
                    .average().orElse(0.0);
            this.stdDevExecutionTime = Math.sqrt(variance);

            // Calculate other averages
            this.avgDbHits = dbHits.stream().mapToLong(Long::longValue).average().orElse(0.0);
            this.avgResultRows = resultRows.stream().mapToLong(Long::longValue).average().orElse(0.0);
            this.avgMemoryUsage = memoryUsage.stream().mapToLong(Long::longValue).average().orElse(0.0);
        }

        public void printStats() {
            System.out.println("=== " + queryName + " Statistics ===");
            System.out.println("Runs: " + runCount);
            System.out.printf("Execution Time (μs) - Avg: %.2f, Min: %.2f, Max: %.2f, StdDev: %.2f%n",
                    avgExecutionTime, minExecutionTime, maxExecutionTime, stdDevExecutionTime);
            System.out.printf("Average DB Hits: %.2f%n", avgDbHits);
            System.out.printf("Average Result Rows: %.2f%n", avgResultRows);
            System.out.printf("Average Memory Usage: %.2f%n", avgMemoryUsage);
            System.out.println();
        }

        // Getters for comparison
        public double getAvgExecutionTime() { return avgExecutionTime; }
        public double getAvgDbHits() { return avgDbHits; }
        public String getQueryName() { return queryName; }
    }

    public QueryMetrics runQuery(String queryName, String cypher, int iterations) {
        QueryMetrics metrics = new QueryMetrics(queryName);
        String profiledQuery = "PROFILE " + cypher;

        System.out.println("Running " + queryName + " " + iterations + " times...");

        try (Session session = driver.session()) {
            // Warm up run (not counted in metrics)
            Result warmupResult = session.run(profiledQuery);
            warmupResult.consume();

            for (int i = 0; i < iterations; i++) {
                Result result = session.run(profiledQuery);

                // Count rows while iterating (before consume())
                long rowCount = 0;
                while (result.hasNext()) {
                    result.next();
                    rowCount++;
                }

                // Now get the summary
                ResultSummary summary = result.consume();

                // Store execution time
                metrics.executionTimes.add(summary.resultAvailableAfter(TimeUnit.MICROSECONDS) +
                        summary.resultConsumedAfter(TimeUnit.MICROSECONDS));

                // Store row count
                metrics.resultRows.add(rowCount);

                // Store profiling data
                if (summary.hasProfile()) {
                    ProfiledPlan plan = summary.profile();
                    metrics.dbHits.add(metrics.getTotalDbHits(plan));
                    metrics.memoryUsage.add(metrics.extractMemoryUsage(plan));
                } else {
                    metrics.dbHits.add(0L);
                    metrics.memoryUsage.add(0L);
                    if (i == 0) { // Only warn once
                        System.out.println("Warning: No profile available for " + queryName);
                    }
                }

                if ((i + 1) % Math.max(1, iterations / 10) == 0) {
                    System.out.printf("Completed %d/%d runs%n", i + 1, iterations);
                }
            }
        } catch (Exception e) {
            System.err.println("Error running query " + queryName + ": " + e.getMessage());
            e.printStackTrace();
        }

        return metrics;
    }

    public void compareQueries(String query1Name, String query1,
                               String query2Name, String query2,
                               int iterations) {

        System.out.println("Starting query comparison with " + iterations + " iterations each");
        System.out.println("=====================================");

        // Run both queries
        QueryMetrics metrics1 = runQuery(query1Name, query1, iterations);
        QueryMetrics metrics2 = runQuery(query2Name, query2, iterations);

        // Get statistics
        QueryStats stats1 = metrics1.getStatistics();
        QueryStats stats2 = metrics2.getStatistics();

        // Print individual statistics
        stats1.printStats();
        stats2.printStats();

        // Print comparison
        printComparison(stats1, stats2);
    }

    private void printComparison(QueryStats stats1, QueryStats stats2) {
        System.out.println("=== COMPARISON ===");

        // Execution time comparison
        double timeDiff = stats2.getAvgExecutionTime() - stats1.getAvgExecutionTime();
        double timeRatio = stats1.getAvgExecutionTime() / stats2.getAvgExecutionTime();

        System.out.printf("Execution Time Difference: %.2f μs%n", timeDiff);
        if (timeDiff > 0) {
            System.out.printf("%s is %.2fx faster than %s%n",
                    stats1.getQueryName(), timeRatio, stats2.getQueryName());
        } else {
            System.out.printf("%s is %.2fx faster than %s%n",
                    stats2.getQueryName(), 1/timeRatio, stats1.getQueryName());
        }

        // DB hits comparison
        double hitsDiff = stats2.getAvgDbHits() - stats1.getAvgDbHits();
        System.out.printf("DB Hits Difference: %.2f%n", hitsDiff);

        if (stats1.getAvgDbHits() > 0 && stats2.getAvgDbHits() > 0) {
            double hitsRatio = stats1.getAvgDbHits() / stats2.getAvgDbHits();
            if (hitsDiff > 0) {
                System.out.printf("%s uses %.2fx fewer DB hits than %s%n",
                        stats1.getQueryName(), hitsRatio, stats2.getQueryName());
            } else {
                System.out.printf("%s uses %.2fx fewer DB hits than %s%n",
                        stats2.getQueryName(), 1/hitsRatio, stats1.getQueryName());
            }
        }

        System.out.println();
    }

    public void close() {
        driver.close();
    }

    public static void main(String[] args) {
        // Configuration - modify these values
        String NEO4J_URI = "neo4j://127.0.0.1:7687";
        String USERNAME = "neo4j";
        String PASSWORD = "gigglebox";
        int ITERATIONS = 100;

        // Your two Cypher queries - modify these
        String QUERY1 = """
            MATCH (root:TypeC {name: "A"})
                MATCH (root)-[*0..]->(descendant)
                RETURN descendant;
            """;

        String QUERY2 = """
                MATCH (a:TypeC {name: "A"})
                    MATCH (c:TypeC)
                    WHERE c.new_id_label >= a.new_id_label\s
                      AND c.new_id_label < a.new_id_label + a.interval_width
                    RETURN c;
            """;

        CypherQueryComparator comparator = new CypherQueryComparator(NEO4J_URI, USERNAME, PASSWORD);

        try {
            comparator.compareQueries("Query 1 (MATCH-WHERE)", QUERY1,
                    "Query 2 (OPTIONAL MATCH)", QUERY2,
                    ITERATIONS);
        } finally {
            comparator.close();
        }
    }
}