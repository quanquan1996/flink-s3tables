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
            // 解析命令行参数
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

            // 设置执行环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // 配置检查点和重启策略
            env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
            System.out.println("Execution environment configured with checkpointing and restart strategy");

            // 创建表环境
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .build();
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

            // 设置配置参数
            tableEnv.getConfig().setSqlDialect(org.apache.flink.table.api.SqlDialect.DEFAULT);
            System.out.println("Table environment created with default SQL dialect");

            // 创建Glue Catalog
            String warehousePath = String.format("arn:aws:s3tables:%s:%s:bucket/%s",
                    awsRegion, accountId, bucketName);

            String createCatalogSQL = String.format(
                    "CREATE CATALOG glue_catalog WITH (" +
                            "'type'='iceberg'," +
                            "'catalog-impl'='software.amazon.s3tables.iceberg.S3TablesCatalog'," +
                            "'warehouse'='%s'," +
                            "'region'='%s'," +
                            "'aws.region'='%s'" +
                            ")", warehousePath, awsRegion, awsRegion);

            System.out.println("Creating Glue Catalog with SQL: " + createCatalogSQL);
            tableEnv.executeSql(createCatalogSQL);
            System.out.println("Glue Catalog created successfully");

            // 使用创建的Catalog
            tableEnv.useCatalog("glue_catalog");
            System.out.println("Using glue_catalog");

            // 列出所有数据库
            System.out.println("Listing all databases:");
            TableResult databases = tableEnv.executeSql("SHOW DATABASES");
            printTableResult(databases, "Databases");

            // 使用特定数据库
            System.out.println("Using database: " + database);
            tableEnv.useDatabase(database);

            // 列出所有表
            System.out.println("Listing all tables in database " + database + ":");
            TableResult tables = tableEnv.executeSql("SHOW TABLES");
            printTableResult(tables, "Tables");

            // 查询表结构
            System.out.println("Describing table structure for " + tableName + ":");
            TableResult tableDesc = tableEnv.executeSql("DESCRIBE " + tableName);
            printTableResult(tableDesc, "Table Structure");

            // 查询表数据
            System.out.println("Querying data from " + tableName + ":");
            TableResult queryResult = tableEnv.executeSql("SELECT * FROM " + tableName + " LIMIT 10");
            printTableResult(queryResult, "Table Data");

            // 执行插入操作
            //if (params.has("insert") && params.getBoolean("insert")) {
            System.out.println("Inserting test data into " + tableName + ":");
            String insertSQL = String.format(
                    "INSERT INTO %s (id,data) VALUES " +
                            "(103, 'test 3 insert'), " +
                            "(104, 'test 4 insert')",
                    tableName);
            System.out.println("Insert SQL: " + insertSQL);

            tableEnv.executeSql(insertSQL);
            System.out.println("Data inserted successfully");

            // 验证插入
            System.out.println("Verifying inserted data:");
            //重新获取catalog
            tableEnv.executeSql(createCatalogSQL);
            TableResult verifyResult = tableEnv.executeSql(
                    "SELECT * FROM " + tableName + " WHERE id IN (103, 104)");
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
