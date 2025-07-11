package org.example;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class IcebergGlueIntegrationTest {

    public static void main(String[] args) {
        try {
            // Parse command line parameters
            ParameterTool params = ParameterTool.fromArgs(args);
            String awsRegion = params.get("region", "us-west-2");
            String accountId = params.get("account", "051826712157");
            String bucketName = params.get("bucket", "testtable");
            String database = params.get("database", "testdb");
            String tableName = params.get("table", "test_table");

            System.out.println("Starting Iceberg Glue Integration Test...");
            System.out.println("Region: " + awsRegion);
            System.out.println("Account ID: " + accountId);
            System.out.println("Bucket: " + bucketName);
            System.out.println("Database: " + database);
            System.out.println("Table: " + tableName);

            // Set up execution environment
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // Configure checkpointing and restart strategy
            env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
            System.out.println("Execution environment configured with checkpointing and restart strategy");

            // Create table environment
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .build();
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

            // Set configuration parameters
            tableEnv.getConfig().setSqlDialect(org.apache.flink.table.api.SqlDialect.DEFAULT);
            System.out.println("Table environment created with default SQL dialect");

            // Create Glue Catalog
            String warehousePath = String.format("arn:aws:s3tables:%s:%s:bucket/%s",
                    awsRegion, accountId, bucketName);

            String createCatalogSQL = String.format(
                    "CREATE CATALOG glue_catalog WITH (" +
                            "'type'='iceberg'," +
                            "'catalog-impl'='software.amazon.s3tables.iceberg.S3TablesCatalog'," +
                            "'warehouse'='arn:aws:s3tables:%s:%s:bucket/%s'," +
                            "'region'='%s'," +
                            "'aws.region'='%s'," +
                            "'cache-enabled'='false'" +  // Disable caching
                            ")", awsRegion, accountId, bucketName, awsRegion, awsRegion);

            System.out.println("Creating Glue Catalog with SQL: " + createCatalogSQL);
            tableEnv.executeSql(createCatalogSQL);
            System.out.println("Glue Catalog created successfully");

            // Use the created Catalog
            tableEnv.useCatalog("glue_catalog");
            System.out.println("Using glue_catalog");

            // List all databases
            System.out.println("Listing all databases:");
            TableResult databases = tableEnv.executeSql("SHOW DATABASES");
            printTableResult(databases, "Databases");

            // Use specific database
            System.out.println("Using database: " + database);
            tableEnv.useDatabase(database);

            // List all tables
            System.out.println("Listing all tables in database " + database + ":");
            TableResult tables = tableEnv.executeSql("SHOW TABLES");
            printTableResult(tables, "Tables");

            // Query table structure
            System.out.println("Describing table structure for " + tableName + ":");
            TableResult tableDesc = tableEnv.executeSql("DESCRIBE " + tableName);
            printTableResult(tableDesc, "Table Structure");

            // Query table data
            System.out.println("Querying data from " + tableName + ":");
            TableResult queryResult = tableEnv.executeSql("SELECT * FROM " + tableName + " LIMIT 10");
            printTableResult(queryResult, "Table Data");

            // Execute insert operation
            //if (params.has("insert") && params.getBoolean("insert")) {
            System.out.println("Inserting test data into " + tableName + ":");
            String insertSQL = String.format(
                    "INSERT INTO %s (id,data) VALUES " +
                            "(109, 'test 3 insert'), " +
                            "(110, 'test 4 insert')",
                    tableName);
            System.out.println("Insert SQL: " + insertSQL);

            tableEnv.executeSql(insertSQL);
            System.out.println("Data inserted successfully");

            // Verify insertion
            System.out.println("Verifying inserted data:");
            TableResult verifyResult = tableEnv.executeSql(
                    "SELECT * FROM " + tableName + " WHERE id IN (109, 110)");
            printTableResult(verifyResult, "Inserted Data");
            //}

            System.out.println("Test completed successfully");

        } catch (Exception e) {
            System.err.println("Error during test execution: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void printTableResult(TableResult result, String title) {
        System.out.println("\n===== " + title + " =====");
        result.print();
        System.out.println("====================\n");
    }
}

