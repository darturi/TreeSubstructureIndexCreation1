import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.Neo4jException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Neo4jDriver implements AutoCloseable {

    private final Driver driver;
    private final TreeGenerator tg = new TreeGenerator();

    public Neo4jDriver(String uri, String user, String password) {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    @Override
    public void close() throws RuntimeException {
        driver.close();
    }

    public static void main(String[] args) {
        // Connection details - adjust these for your Neo4j instance
        String uri = "neo4j://127.0.0.1:7687";  // Default Neo4j bolt port
        String user = "neo4j";
        String password = "gigglebox";  // Change this to your actual password

        try (Neo4jDriver app = new Neo4jDriver(uri, user, password)) {
            app.testConnection();
            app.createSampleData(2);
            // app.getAllNodes();
            // app.addLabelToNodeAndDescendants("TypeA", "name", "A", "TypeC");
            // app.annotateDepthAndHeight("name", "A", "TypeC");
            // app.handleSpaceRequests("name", "A", "TypeC");
            // app.handleIdAssignment("name","A", "TypeC", 1.0, "new_id_label");

            app.complete_annotation(
                    "TypeA",
                    "name",
                    "A",
                    "TypeC",
                    1.0,
                    "new_id_label"
            );



            // app.deleteData();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // RELATIONSHIP AGNOSTIC
    public void complete_annotation(String rootTypeLabel,
                                    String idParamName,
                                    String idParamValue,
                                    String nodeMemberLabel,
                                    double rootId,
                                    String newIdName
                                    ){
        addLabelToNodeAndDescendants(
                rootTypeLabel,
                idParamName,
                idParamValue,
                nodeMemberLabel
        );
        annotateDepthAndHeight(
                idParamName,
                idParamValue,
                nodeMemberLabel
        );
        handleSpaceRequests(
                idParamName,
                idParamValue,
                nodeMemberLabel
        );
        handleIdAssignment(
                idParamName,
                idParamValue,
                nodeMemberLabel,
                rootId,
                newIdName
        );

    }

    public void testConnection() {
        try (Session session = driver.session()) {
            var record = session.executeRead(tx ->
                    tx.run(
                            "CALL dbms.components() " +
                                    "YIELD name, versions, edition " +
                                    "WHERE name = 'Neo4j Kernel' " +
                                    "RETURN name, versions[0] as version, edition"
                    ).single()
            );

            System.out.println("Connected to: " +
                    record.get("name").asString() + " " +
                    record.get("version").asString() + " (" +
                    record.get("edition").asString() + ")"
            );
        } catch (Neo4jException e) {
            System.err.println("Failed to connect: " + e.getMessage());
            throw e;
        }
    }

    public void createSampleData(int createCommand) {
        try (Session session = driver.session()) {
            session.executeWrite(tx -> {
                // Clear existing data (be careful with this in production!)
                tx.run("MATCH (n) DETACH DELETE n");

                if (createCommand == 1) {
                    tx.run(this.tg.generate_tree_1());
                }
                else if (createCommand == 2) {
                    tx.run(this.tg.generate_tree_2());
                }

                System.out.println("Sample data created successfully!");
                return null;
            });
        }
    }

    public void queryData() {
        System.out.println("\n=== Querying Data ===");
    }

    // RELATIONSHIP AGNOSTIC
    public int addLabelToNodeAndDescendants(String rootTypeLabel, String idParamName,
                                            String idParamValue, String nodeMemberLabel) {

        // Validate input parameters
        if (rootTypeLabel == null || rootTypeLabel.trim().isEmpty() ||
                idParamName == null || idParamName.trim().isEmpty() ||
                idParamValue == null || idParamValue.trim().isEmpty() ||
                nodeMemberLabel == null || nodeMemberLabel.trim().isEmpty()) {
            throw new IllegalArgumentException("All parameters must be non-null and non-empty");
        }

        // Build the Cypher query
        String cypherQuery = String.format(
                "MATCH (root:%s {%s: $idParamValue}) " +
                        "CALL { " +
                        "WITH root " +
                        "MATCH (descendant) " +
                        "WHERE root = descendant OR (root)-[*1..]->(descendant) " +
                        "SET descendant:%s " +
                        "RETURN count(descendant) as nodeCount " +
                        "} " +
                        "RETURN nodeCount",
                rootTypeLabel, idParamName, nodeMemberLabel
        );

        // Execute the query
        try (Session session = driver.session()) {
            return session.executeWrite(tx -> {
                Map<String, Object> parameters = new HashMap<>();
                parameters.put("idParamValue", idParamValue);

                Result result = tx.run(cypherQuery, parameters);

                if (result.hasNext()) {
                    org.neo4j.driver.Record record = result.next();
                    return record.get("nodeCount").asInt();
                }
                return 0;
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to add labels to node and descendants: " + e.getMessage(), e);
        }
    }

    // SINGLE RELATIONSHIP TYPE
    public void annotateDepthAndHeight(String idParamName,
                                       String idParamValue,
                                       String nodeMemberLabel,
                                       String relType) {
        try (Session session = driver.session()) {
            session.executeWrite(tx -> {
                String cypher = """
                    // Find the dynamic-label root by dynamic property key/value
                    MATCH (root)
                    WHERE $nodeMemberLabel IN labels(root)
                      AND root[$idParamName] = $idParamValue
    
                    // Paths from root to any node; only traverse rels of the given type,
                    // and only consider endpoints with the given label
                    MATCH p=(root)-[rels*0..]->(n)
                    WHERE ALL(r IN rels WHERE type(r) = $relType)
                      AND $nodeMemberLabel IN labels(n)
    
                    // Depth = shortest path length from root to n (across the allowed rels)
                    WITH root, n, min(length(p)) AS depth
                    SET n.depth = depth
    
                    // Height = longest path length from n to a leaf
                    WITH n
                    OPTIONAL MATCH q=(n)-[rels2*0..]->(leaf)
                    WHERE ALL(r2 IN rels2 WHERE type(r2) = $relType)
                      AND $nodeMemberLabel IN labels(leaf)
                      AND NOT EXISTS {
                        MATCH (leaf)-[r3]->(c3)
                        WHERE type(r3) = $relType AND $nodeMemberLabel IN labels(c3)
                      }
    
                    WITH n, coalesce(max(length(q)), 0) AS height
                    SET n.height = height
                """;

                Map<String, Object> params = Map.of(
                        "idParamName", idParamName,
                        "idParamValue", idParamValue,
                        "nodeMemberLabel", nodeMemberLabel,
                        "relType", relType
                );

                tx.run(cypher, params);
                return null;
            });
        } catch (Neo4jException e) {
            System.err.println("Failed to annotate depth/height for "
                    + nodeMemberLabel + "[" + idParamName + "=" + idParamValue + "]: " + e.getMessage());
            throw e;
        }
    }
    // TODO: TEST --> RELATIONSHIP AGNOSTIC
    public void annotateDepthAndHeight(String idParamName,
                                       String idParamValue,
                                       String nodeMemberLabel) {
        try (Session session = driver.session()) {
            session.executeWrite(tx -> {
                String cypher = """
                // Find the dynamic-label root by dynamic property key/value
                MATCH (root)
                WHERE $nodeMemberLabel IN labels(root)
                  AND root[$idParamName] = $idParamValue

                // Paths from root to any node; traverse ANY outgoing relationships,
                // and only consider endpoints with the given label
                MATCH p=(root)-[*0..]->(n)
                WHERE $nodeMemberLabel IN labels(n)

                // Depth = shortest path length from root to n (across any rels)
                WITH root, n, min(length(p)) AS depth
                SET n.depth = depth

                // Height = longest path length from n to a leaf
                WITH n
                OPTIONAL MATCH q=(n)-[*0..]->(leaf)
                WHERE $nodeMemberLabel IN labels(leaf)
                  AND NOT EXISTS {
                    MATCH (leaf)-[]->(c3)
                    WHERE $nodeMemberLabel IN labels(c3)
                  }

                WITH n, coalesce(max(length(q)), 0) AS height
                SET n.height = height
            """;

                Map<String, Object> params = Map.of(
                        "idParamName", idParamName,
                        "idParamValue", idParamValue,
                        "nodeMemberLabel", nodeMemberLabel
                );

                tx.run(cypher, params);
                return null;
            });
        } catch (Neo4jException e) {
            System.err.println("Failed to annotate depth/height for "
                    + nodeMemberLabel + "[" + idParamName + "=" + idParamValue + "]: " + e.getMessage());
            throw e;
        }
    }

    // TODO: RELATIONSHIP LIST

    public void getAllNodes() {
        try (Session session = driver.session()) {
            session.executeRead(tx -> {
                var result = tx.run("MATCH (n) RETURN n");

                System.out.println("\n=== All Nodes in the Graph ===");
                while (result.hasNext()) {
                    var record = result.next();
                    var node = record.get("n").asNode();
                    System.out.println("Node: " + node.labels() + " " + node.asMap());
                }
                return null;
            });
        } catch (Neo4jException e) {
            System.err.println("Error retrieving nodes: " + e.getMessage());
            throw e;
        }
    }


    // SINGLE RELATIONSHIP TYPE
    public void handleSpaceRequests(String idParamName,
                                    String idParamValue,
                                    String nodeMemberLabel,
                                    String relType) {
        try (Session session = driver.session()) {
            session.executeWrite(tx -> {
                // Step 1: Set interval_width = 1 for all leaf nodes in the subtree
                // - Root is any node that has label = nodeMemberLabel and root[idParamName] = idParamValue
                // - Traverse only relationships whose type = relType (checked via type(r))
                // - Leaf = no outgoing rel of type relType to a node with label nodeMemberLabel
                var leafQuery = """
                    MATCH (root)
                    WHERE $nodeMemberLabel IN labels(root)
                      AND root[$idParamName] = $idParamValue
                    MATCH p=(root)-[rels*0..]->(leaf)
                    WHERE ALL(r IN rels WHERE type(r) = $relType)
                      AND $nodeMemberLabel IN labels(leaf)
                      AND NOT EXISTS {
                        MATCH (leaf)-[r2]->(c2)
                        WHERE type(r2) = $relType AND $nodeMemberLabel IN labels(c2)
                      }
                    WITH DISTINCT leaf
                    SET leaf.interval_width = 1
                    RETURN count(leaf) AS leafCount
                """;

                Map<String, Object> leafParams = Map.of(
                        "idParamName", idParamName,
                        "idParamValue", idParamValue,
                        "nodeMemberLabel", nodeMemberLabel,
                        "relType", relType
                );
                var leafResult = tx.run(leafQuery, leafParams);
                int leafCount = leafResult.single().get("leafCount").asInt();
                System.out.println("Set interval_width = 1 for " + leafCount + " leaf nodes");

                // Step 2: Bottom-up processing until no more parents can be set
                int iteration = 0;
                int processedInIteration;

                do {
                    iteration++;
                    var processQuery = """
                        MATCH (root)
                        WHERE $nodeMemberLabel IN labels(root)
                          AND root[$idParamName] = $idParamValue
                        MATCH p=(root)-[rels*0..]->(parent)
                        WHERE ALL(r IN rels WHERE type(r) = $relType)
                          AND $nodeMemberLabel IN labels(parent)
                        WITH DISTINCT parent
                        WHERE parent.interval_width IS NULL
                          AND EXISTS {
                            MATCH (parent)-[r]->(c)
                            WHERE type(r) = $relType AND $nodeMemberLabel IN labels(c)
                          }
                        WITH parent,
                             [ (parent)-[r]->(c)
                               WHERE type(r) = $relType AND $nodeMemberLabel IN labels(c) | c ] AS children
                        WHERE ALL(child IN children WHERE child.interval_width IS NOT NULL)
                        WITH parent,
                             reduce(space_request = 0.0, child IN children |
                                space_request + coalesce(child.interval_width, 0.0)) AS space_request
                        SET parent.interval_width =
                             toFloat(toInteger(ceil(toFloat(space_request)/10.0)) * 10) / 10.0
                        RETURN count(parent) AS processed
                    """;

                    Map<String, Object> processParams = Map.of(
                            "idParamName", idParamName,
                            "idParamValue", idParamValue,
                            "nodeMemberLabel", nodeMemberLabel,
                            "relType", relType
                    );

                    var processResult = tx.run(processQuery, processParams);
                    processedInIteration = processResult.single().get("processed").asInt();

                    if (processedInIteration > 0) {
                        System.out.println("Iteration " + iteration + ": Processed " + processedInIteration + " nodes");
                    }

                } while (processedInIteration > 0);

                System.out.println("Bottom-up processing completed in " + iteration + " iterations");

                // Step 3: Verify results by showing final interval_width values
                var verifyQuery = """
                    MATCH (root)
                    WHERE $nodeMemberLabel IN labels(root)
                      AND root[$idParamName] = $idParamValue
                    MATCH p=(root)-[rels*0..]->(node)
                    WHERE ALL(r IN rels WHERE type(r) = $relType)
                      AND $nodeMemberLabel IN labels(node)
                    WITH DISTINCT node
                    RETURN node[$idParamName] AS nodeId, node.interval_width AS intervalWidth
                    ORDER BY nodeId
                """;

                Map<String, Object> verifyParams = Map.of(
                        "idParamName", idParamName,
                        "idParamValue", idParamValue,
                        "nodeMemberLabel", nodeMemberLabel,
                        "relType", relType
                );

                var verifyResult = tx.run(verifyQuery, verifyParams);

                System.out.println("\n=== Final interval_width values for subtree rooted at "
                        + nodeMemberLabel + "[" + idParamName + "=" + idParamValue + "] ===");
                while (verifyResult.hasNext()) {
                    var record = verifyResult.next();
                    var nodeId = record.get("nodeId").asString("");
                    var intervalWidth = record.get("intervalWidth").asDouble();
                    System.out.println("Node: " + nodeId + " -> interval_width: " + intervalWidth);
                }

                return null;
            });
        } catch (Neo4jException e) {
            System.err.println("Error processing space requests for root "
                    + nodeMemberLabel + "[" + idParamName + "=" + idParamValue + "]: " + e.getMessage());
            throw e;
        }
    }

    // TODO: TEST --> RELATIONSHIP AGNOSTIC
    public void handleSpaceRequests(String idParamName,
                                    String idParamValue,
                                    String nodeMemberLabel) {
        try (Session session = driver.session()) {
            session.executeWrite(tx -> {
                // Step 1: Set interval_width = 1 for all leaf nodes in the subtree
                // - Root is any node that has label = nodeMemberLabel and root[idParamName] = idParamValue
                // - Traverse any outgoing relationship (no type filter)
                // - Leaf = no outgoing relationship to a node with label nodeMemberLabel
                var leafQuery = """
                MATCH (root)
                WHERE $nodeMemberLabel IN labels(root)
                  AND root[$idParamName] = $idParamValue
                MATCH p=(root)-[*0..]->(leaf)
                WHERE $nodeMemberLabel IN labels(leaf)
                  AND NOT EXISTS {
                    MATCH (leaf)-[]->(c2)
                    WHERE $nodeMemberLabel IN labels(c2)
                  }
                WITH DISTINCT leaf
                SET leaf.interval_width = 1
                RETURN count(leaf) AS leafCount
            """;

                Map<String, Object> leafParams = Map.of(
                        "idParamName", idParamName,
                        "idParamValue", idParamValue,
                        "nodeMemberLabel", nodeMemberLabel
                );
                var leafResult = tx.run(leafQuery, leafParams);
                int leafCount = leafResult.single().get("leafCount").asInt();
                System.out.println("Set interval_width = 1 for " + leafCount + " leaf nodes");

                // Step 2: Bottom-up processing until no more parents can be set
                int iteration = 0;
                int processedInIteration;

                do {
                    iteration++;
                    var processQuery = """
                    MATCH (root)
                    WHERE $nodeMemberLabel IN labels(root)
                      AND root[$idParamName] = $idParamValue
                    MATCH p=(root)-[*0..]->(parent)
                    WHERE $nodeMemberLabel IN labels(parent)
                    WITH DISTINCT parent
                    WHERE parent.interval_width IS NULL
                      AND EXISTS {
                        MATCH (parent)-[]->(c)
                        WHERE $nodeMemberLabel IN labels(c)
                      }
                    WITH parent,
                         [ (parent)-[]->(c)
                           WHERE $nodeMemberLabel IN labels(c) | c ] AS children
                    WHERE ALL(child IN children WHERE child.interval_width IS NOT NULL)
                    WITH parent,
                         reduce(space_request = 0.0, child IN children |
                            space_request + coalesce(child.interval_width, 0.0)) AS space_request
                    SET parent.interval_width =
                         toFloat(toInteger(ceil(toFloat(space_request)/10.0)) * 10) / 10.0
                    RETURN count(parent) AS processed
                """;

                    Map<String, Object> processParams = Map.of(
                            "idParamName", idParamName,
                            "idParamValue", idParamValue,
                            "nodeMemberLabel", nodeMemberLabel
                    );

                    var processResult = tx.run(processQuery, processParams);
                    processedInIteration = processResult.single().get("processed").asInt();

                    if (processedInIteration > 0) {
                        System.out.println("Iteration " + iteration + ": Processed " + processedInIteration + " nodes");
                    }

                } while (processedInIteration > 0);

                System.out.println("Bottom-up processing completed in " + iteration + " iterations");

                // Step 3: Verify results by showing final interval_width values
                var verifyQuery = """
                MATCH (root)
                WHERE $nodeMemberLabel IN labels(root)
                  AND root[$idParamName] = $idParamValue
                MATCH p=(root)-[*0..]->(node)
                WHERE $nodeMemberLabel IN labels(node)
                WITH DISTINCT node
                RETURN node[$idParamName] AS nodeId, node.interval_width AS intervalWidth
                ORDER BY nodeId
            """;

                Map<String, Object> verifyParams = Map.of(
                        "idParamName", idParamName,
                        "idParamValue", idParamValue,
                        "nodeMemberLabel", nodeMemberLabel
                );

                var verifyResult = tx.run(verifyQuery, verifyParams);

                System.out.println("\n=== Final interval_width values for subtree rooted at "
                        + nodeMemberLabel + "[" + idParamName + "=" + idParamValue + "] ===");
                while (verifyResult.hasNext()) {
                    var record = verifyResult.next();
                    var nodeId = record.get("nodeId").asString("");
                    var intervalWidth = record.get("intervalWidth").asDouble();
                    System.out.println("Node: " + nodeId + " -> interval_width: " + intervalWidth);
                }

                return null;
            });
        } catch (Neo4jException e) {
            System.err.println("Error processing space requests for root "
                    + nodeMemberLabel + "[" + idParamName + "=" + idParamValue + "]: " + e.getMessage());
            throw e;
        }
    }

    // TODO: RELATIONSHIP LIST


    // SINGLE RELATIONSHIP TYPE
    public void handleIdAssignment(String idParamName,
                                   String idParamValue,
                                   String nodeMemberLabel,
                                   String relType,
                                   double rootId,
                                   String idName) {
        try (Session session = driver.session()) {
            session.executeWrite(tx -> {
                    // Step 1: Initialize root node and calculate depths
                    var initQuery = """
                    MATCH (root)
                    WHERE $nodeMemberLabel IN labels(root)
                      AND root[$idParamName] = $idParamValue
                    SET root[$idName] = toFloat($rootId)
    
                    WITH root
                    MATCH p=(root)-[rels*0..]->(node)
                    WHERE ALL(r IN rels WHERE type(r) = $relType)
                      AND $nodeMemberLabel IN labels(node)
                    WITH root, node, length(p) AS node_depth
                    SET node.depth = node_depth
    
                    RETURN count(node) AS nodesProcessed, max(node_depth) AS maxDepth
                """;

                Map<String, Object> initParams = Map.of(
                        "idParamName", idParamName,
                        "idParamValue", idParamValue,
                        "nodeMemberLabel", nodeMemberLabel,
                        "relType", relType,
                        "rootId", rootId,
                        "idName", idName
                );

                var initResult = tx.run(initQuery, initParams);
                var initRecord = initResult.single();
                int nodesProcessed = initRecord.get("nodesProcessed").asInt();
                int maxDepth = initRecord.get("maxDepth").asInt();

                System.out.println("Initialized " + nodesProcessed + " nodes with depths (max depth: " + maxDepth + ")");
                System.out.println("Set root " + nodeMemberLabel + "[" + idParamName + "=" + idParamValue + "] "
                        + idName + " = " + rootId);

                // Step 2: Process nodes level by level iteratively
                for (int currentLevel = 1; currentLevel <= maxDepth; currentLevel++) {
                    var levelProcessQuery = """
                        MATCH (root)
                        WHERE $nodeMemberLabel IN labels(root)
                          AND root[$idParamName] = $idParamValue
    
                        // Find parents of this level from the subtree
                        MATCH p=(root)-[rels*0..]->(parent)
                        WHERE ALL(r IN rels WHERE type(r) = $relType)
                          AND $nodeMemberLabel IN labels(parent)
                          AND parent.depth = $parentDepth
                          AND parent[$idName] IS NOT NULL
    
                        // Ensure parent has children of the same label via the relType
                        WITH DISTINCT parent
                        WHERE EXISTS {
                          MATCH (parent)-[r]->(c)
                          WHERE type(r) = $relType AND $nodeMemberLabel IN labels(c)
                        }
    
                        WITH parent,
                             (10.0 ^ -toFloat(parent.depth + 1)) AS next_place,
                             [ (parent)-[r]->(child)
                               WHERE type(r) = $relType AND $nodeMemberLabel IN labels(child) | child ] AS children,
                             toFloat(parent.depth + 1) AS child_depth
    
                        // Precompute rounding factor for this level
                        WITH parent, next_place, children, child_depth,
                             10.0 ^ child_depth AS rounding_factor
    
                        UNWIND range(0, size(children) - 1) AS childIndex
                        WITH parent, next_place, children, childIndex, child_depth, rounding_factor,
                             children[childIndex] AS currentChild,
                             reduce(total_offset = 0.0,
                                prev_idx IN range(0, childIndex - 1) |
                                total_offset + (next_place * coalesce(children[prev_idx].interval_width, 0.0))
                             ) AS accumulated_offset
    
                        // Calculate and round child's id to control error accumulation
                        WITH currentChild,
                             parent[$idName] + next_place + accumulated_offset AS raw_id,
                             rounding_factor, $idName AS idNameParam
    
                        SET currentChild[idNameParam] = toFloat(round(raw_id * rounding_factor) / rounding_factor)
    
                        RETURN count(currentChild) AS childrenProcessed
                    """;

                    Map<String, Object> levelParams = Map.of(
                            "idParamName", idParamName,
                            "idParamValue", idParamValue,
                            "nodeMemberLabel", nodeMemberLabel,
                            "relType", relType,
                            "parentDepth", currentLevel - 1,
                            "idName", idName
                    );

                    var levelResult = tx.run(levelProcessQuery, levelParams);

                    int childrenProcessed = 0;
                    if (levelResult.hasNext()) {
                        childrenProcessed = levelResult.stream()
                                .mapToInt(record -> record.get("childrenProcessed").asInt())
                                .sum();
                    }

                    if (childrenProcessed > 0) {
                        System.out.println("Level " + currentLevel + ": Assigned " + idName + " to " + childrenProcessed + " nodes");
                    } else if (currentLevel == 1) {
                        System.out.println("No children found at level 1 - tree may be a single node");
                    }
                }

                System.out.println("ID assignment processing completed for " + maxDepth + " levels");

                // Step 3: Verify results by showing assigned id values
                var verifyQuery = """
                    MATCH (root)
                    WHERE $nodeMemberLabel IN labels(root)
                      AND root[$idParamName] = $idParamValue
                    MATCH p=(root)-[rels*0..]->(node)
                    WHERE ALL(r IN rels WHERE type(r) = $relType)
                      AND $nodeMemberLabel IN labels(node)
                    WITH DISTINCT node
                    RETURN node[$idParamName] AS nodeKey,
                           node.depth AS depth,
                           node[$idName] AS nodeId,
                           node.interval_width AS intervalWidth
                    ORDER BY depth, nodeId
                """;

                Map<String, Object> verifyParams = Map.of(
                        "idParamName", idParamName,
                        "idParamValue", idParamValue,
                        "nodeMemberLabel", nodeMemberLabel,
                        "relType", relType,
                        "idName", idName
                );

                var verifyResult = tx.run(verifyQuery, verifyParams);

                System.out.println("\n=== " + idName + " assignments for subtree rooted at "
                        + nodeMemberLabel + "[" + idParamName + "=" + idParamValue + "] ===");
                System.out.printf("%-24s %-8s %-16s %-15s%n", "Node Key", "Depth", idName, "Interval Width");
                System.out.println("-".repeat(70));

                while (verifyResult.hasNext()) {
                    var record = verifyResult.next();
                    var nodeKey = record.get("nodeKey").asString("");
                    var depth = record.get("depth").asInt();
                    var nodeId = record.get("nodeId").isNull() ? null : record.get("nodeId").asDouble();
                    var intervalWidth = record.get("intervalWidth").isNull() ? null : record.get("intervalWidth").asDouble();

                    System.out.printf("%-24s %-8d %-16s %-15s%n",
                            nodeKey,
                            depth,
                            nodeId == null ? "null" : String.format("%.6f", nodeId),
                            intervalWidth == null ? "null" : String.format("%.1f", intervalWidth));
                }

                return null;
            });
        } catch (Neo4jException e) {
            System.err.println("Error processing ID assignment for root "
                    + nodeMemberLabel + "[" + idParamName + "=" + idParamValue + "]: " + e.getMessage());
            throw e;
        }
    }
    // TODO: TEST --> RELATIONSHIP AGNOSTIC
    public void handleIdAssignment(String idParamName,
                                   String idParamValue,
                                   String nodeMemberLabel,
                                   double rootId,
                                   String idName) {
        try (Session session = driver.session()) {
            session.executeWrite(tx -> {
                // Step 1: Initialize root node and calculate depths
                var initQuery = """
                MATCH (root)
                WHERE $nodeMemberLabel IN labels(root)
                  AND root[$idParamName] = $idParamValue
                SET root[$idName] = toFloat($rootId)

                WITH root
                MATCH p=(root)-[*0..]->(node)
                WHERE $nodeMemberLabel IN labels(node)
                WITH root, node, length(p) AS node_depth
                SET node.depth = node_depth

                RETURN count(node) AS nodesProcessed, max(node_depth) AS maxDepth
            """;

                Map<String, Object> initParams = Map.of(
                        "idParamName", idParamName,
                        "idParamValue", idParamValue,
                        "nodeMemberLabel", nodeMemberLabel,
                        "rootId", rootId,
                        "idName", idName
                );

                var initResult = tx.run(initQuery, initParams);
                var initRecord = initResult.single();
                int nodesProcessed = initRecord.get("nodesProcessed").asInt();
                int maxDepth = initRecord.get("maxDepth").asInt();

                System.out.println("Initialized " + nodesProcessed + " nodes with depths (max depth: " + maxDepth + ")");
                System.out.println("Set root " + nodeMemberLabel + "[" + idParamName + "=" + idParamValue + "] "
                        + idName + " = " + rootId);

                // Step 2: Process nodes level by level iteratively
                for (int currentLevel = 1; currentLevel <= maxDepth; currentLevel++) {
                    var levelProcessQuery = """
                    MATCH (root)
                    WHERE $nodeMemberLabel IN labels(root)
                      AND root[$idParamName] = $idParamValue

                    // Find parents of this level from the subtree
                    MATCH p=(root)-[*0..]->(parent)
                    WHERE $nodeMemberLabel IN labels(parent)
                      AND parent.depth = $parentDepth
                      AND parent[$idName] IS NOT NULL

                    // Ensure parent has children of the same label via any outgoing relationship
                    WITH DISTINCT parent
                    WHERE EXISTS {
                      MATCH (parent)-[]->(c)
                      WHERE $nodeMemberLabel IN labels(c)
                    }

                    WITH parent,
                         (10.0 ^ -toFloat(parent.depth + 1)) AS next_place,
                         [ (parent)-[]->(child)
                           WHERE $nodeMemberLabel IN labels(child) | child ] AS children,
                         toFloat(parent.depth + 1) AS child_depth

                    // Precompute rounding factor for this level
                    WITH parent, next_place, children, child_depth,
                         10.0 ^ child_depth AS rounding_factor

                    UNWIND range(0, size(children) - 1) AS childIndex
                    WITH parent, next_place, children, childIndex, child_depth, rounding_factor,
                         children[childIndex] AS currentChild,
                         reduce(total_offset = 0.0,
                            prev_idx IN range(0, childIndex - 1) |
                            total_offset + (next_place * coalesce(children[prev_idx].interval_width, 0.0))
                         ) AS accumulated_offset

                    // Calculate and round child's id to control error accumulation
                    WITH currentChild,
                         parent[$idName] + next_place + accumulated_offset AS raw_id,
                         rounding_factor, $idName AS idNameParam

                    SET currentChild[idNameParam] = toFloat(round(raw_id * rounding_factor) / rounding_factor)

                    RETURN count(currentChild) AS childrenProcessed
                """;

                    Map<String, Object> levelParams = Map.of(
                            "idParamName", idParamName,
                            "idParamValue", idParamValue,
                            "nodeMemberLabel", nodeMemberLabel,
                            "parentDepth", currentLevel - 1,
                            "idName", idName
                    );

                    var levelResult = tx.run(levelProcessQuery, levelParams);

                    int childrenProcessed = 0;
                    if (levelResult.hasNext()) {
                        childrenProcessed = levelResult.stream()
                                .mapToInt(record -> record.get("childrenProcessed").asInt())
                                .sum();
                    }

                    if (childrenProcessed > 0) {
                        System.out.println("Level " + currentLevel + ": Assigned " + idName + " to " + childrenProcessed + " nodes");
                    } else if (currentLevel == 1) {
                        System.out.println("No children found at level 1 - tree may be a single node");
                    }
                }

                System.out.println("ID assignment processing completed for " + maxDepth + " levels");

                // Step 3: Verify results by showing assigned id values
                var verifyQuery = """
                MATCH (root)
                WHERE $nodeMemberLabel IN labels(root)
                  AND root[$idParamName] = $idParamValue
                MATCH p=(root)-[*0..]->(node)
                WHERE $nodeMemberLabel IN labels(node)
                WITH DISTINCT node
                RETURN node[$idParamName] AS nodeKey,
                       node.depth AS depth,
                       node[$idName] AS nodeId,
                       node.interval_width AS intervalWidth
                ORDER BY depth, nodeId
            """;

                Map<String, Object> verifyParams = Map.of(
                        "idParamName", idParamName,
                        "idParamValue", idParamValue,
                        "nodeMemberLabel", nodeMemberLabel,
                        "idName", idName
                );

                var verifyResult = tx.run(verifyQuery, verifyParams);

                System.out.println("\n=== " + idName + " assignments for subtree rooted at "
                        + nodeMemberLabel + "[" + idParamName + "=" + idParamValue + "] ===");
                System.out.printf("%-24s %-8s %-16s %-15s%n", "Node Key", "Depth", idName, "Interval Width");
                System.out.println("-".repeat(70));

                while (verifyResult.hasNext()) {
                    var record = verifyResult.next();
                    var nodeKey = record.get("nodeKey").asString("");
                    var depth = record.get("depth").asInt();
                    var nodeIdVal = record.get("nodeId").isNull() ? null : record.get("nodeId").asDouble();
                    var intervalWidth = record.get("intervalWidth").isNull() ? null : record.get("intervalWidth").asDouble();

                    System.out.printf("%-24s %-8d %-16s %-15s%n",
                            nodeKey,
                            depth,
                            nodeIdVal == null ? "null" : String.format("%.6f", nodeIdVal),
                            intervalWidth == null ? "null" : String.format("%.1f", intervalWidth));
                }

                return null;
            });
        } catch (Neo4jException e) {
            System.err.println("Error processing ID assignment for root "
                    + nodeMemberLabel + "[" + idParamName + "=" + idParamValue + "]: " + e.getMessage());
            throw e;
        }
    }

    // TODO: RELATIONSHIP LIST

    public void deleteData() {
        System.out.println("\n=== Deleting Data ===");

        try (Session session = driver.session()) {
            session.executeWrite(tx -> {
                tx.run("MATCH ()-[r]-() DELETE r");
                System.out.println("All nodes and relationships deleted successfully!");
                return null;
            });
        } catch (Neo4jException e) {
            System.err.println("Error deleting data: " + e.getMessage());
            throw e;
        }

        try (Session session = driver.session()) {
            session.executeWrite(tx -> {
                tx.run("MATCH (n) DELETE n");
                System.out.println("All nodes and relationships deleted successfully!");
                return null;
            });
        } catch (Neo4jException e) {
            System.err.println("Error deleting data: " + e.getMessage());
            throw e;
        }
    }
}