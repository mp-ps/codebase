package org.example;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Objects;

public class CloudSqlBatchProcession {
    public static void main(String[] args) {
        pipeline();
    }

    private static void pipeline() {
        // Define pipeline options

        String[] arguments = {"--runner=DirectRunner", "--tempLocation=gs://cardnet_2", "--project=burner-mihparma"};
        PipelineOptions options = PipelineOptionsFactory.fromArgs(arguments).withValidation().as(PipelineOptions.class);

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        // Set the source and destination BigQuery tables
        String sourceTable = "burner-mihparma:raw.CardnetDimensionRelationshipManager";
        String destinationTable = "burner-mihparma:stage.dimension_relationship_manager";

        // Step 1: Read from the source BigQuery table
        PCollection<TableRow> sourceData = pipeline
                .apply("ReadFromBigQuery", BigQueryIO.readTableRows().from(sourceTable));

        // Step 1: Assign a constant key (e.g., "1") to all rows
        PCollection<KV<String, TableRow>> keyedRows = sourceData.apply(
                WithKeys.of((TableRow row) -> "1")  // Assign the constant key "1"
        ).setCoder(KvCoder.of(StringUtf8Coder.of(), TableRowJsonCoder.of())); // Set the coder for KV<String, TableRow>


        // Step 2: Apply GroupIntoBatches with a batch size of 500
        PCollection<KV<String, Iterable<TableRow>>> batchedRows = keyedRows.apply(
                GroupIntoBatches.ofSize(3)
        );

        // Step 3: Remove the key and keep only the batched elements
        PCollection<Iterable<TableRow>> batchedRowsWithoutKey = batchedRows.apply(Values.create());

        // Process each batch and generate a SQL query for each
        PCollection<String> sqlQueries = batchedRowsWithoutKey
                .apply(ParDo.of(new ProcessBatchFn()));

        // Output the SQL queries
        sqlQueries.apply(ParDo.of(new SQLQueryOutputFn()));

        PipelineResult result = pipeline.run();
        // Wait for the pipeline to finish
        result.waitUntilFinish();
    }
    // Process each batch and output the SQL query directly
    static class ProcessBatchFn extends DoFn<Iterable<TableRow>, String> {

        @ProcessElement
        public void processElement(@Element Iterable<TableRow> batch, OutputReceiver<String> out) {
            StringBuilder sql = new StringBuilder("INSERT INTO my_table (relationship_manager_id, name, team) VALUES ");
            StringBuilder updateClause = new StringBuilder();

            for (TableRow row : batch) {
                assert row != null;

                String value1 = (row.get("relationship_manager_id") == null) ? "NULL" : "'" + row.get("relationship_manager_id").toString() + "'";
                String value2 = (row.get("name") == null) ? "NULL" : "'" + row.get("name").toString() + "'";
                String value3 = (row.get("team") == null) ? "NULL" : "'" + row.get("team").toString() + "'";

                sql.append(String.format("(%s,%s,%s),", value1, value2, value3));
            }

            // Remove the last comma from the VALUES part
            sql.setLength(sql.length() - 1);

            // Construct the ON DUPLICATE KEY UPDATE clause
            updateClause.append("name = VALUES(name), team = VALUES(team)");

            sql.append(" ON DUPLICATE KEY UPDATE ").append(updateClause).append(";");

            // Print the SQL query
            System.out.println(sql);

            // Output the SQL query
            out.output(sql.toString());
        }
    }
    static class SQLQueryOutputFn extends DoFn<String, Void> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String sqlQuery = c.element();
            // Handle the SQL query, e.g., send it to a database
            System.out.println("Executing SQL: " + sqlQuery);
            // You can add your database execution logic here
        }
    }
}
