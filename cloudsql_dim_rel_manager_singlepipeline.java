package org.example;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.Objects;

public class cloudsql_dim_rel_manager_singlepipeline {
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

        //STEP : 2 PRINT SOURCE DATA
        sourceData.apply("PrintData", MapElements.into(TypeDescriptor.of(Void.class))
                .via((TableRow row) -> {
                    System.out.println(row.toString());
                    return null;
                })
        );

        // Step 3: Write to the destination BigQuery table
        sourceData
                .apply("WriteToBigQuery", BigQueryIO.writeTableRows()
                        .to(destinationTable)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                );

        // STEP 4 : Cloud SQL Query formation
        // Transform to a PCollection of KV pairs
        PCollection<KV<Void, String>> valueStrings = sourceData.apply(ParDo.of(new DoFn<TableRow, KV<Void, String>>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                TableRow row = c.element();
                assert row != null;
                String value1 = (row.get("relationship_manager_id") == null) ? "NULL" : "'" + row.get("relationship_manager_id").toString() + "'";//(String) row.get("relationship_manager_id");
                String value2 = (row.get("name") == null) ? "NULL" : "'" + row.get("name").toString() + "'";//row.get("name").toString();
                String value3 = (row.get("team") == null) ? "NULL" : "'" + row.get("team").toString() + "'";//row.get("team").toString();
                c.output(KV.of(null, String.format("(%s,%s, %s)", value1, value2, value3)));
            }
        }));

        // Group the value strings into a single SQL query
        PCollection<String> sqlQueries = valueStrings
                .apply(GroupByKey.create())
                .apply(ParDo.of(new DoFn<KV<Void, Iterable<String>>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        Iterable<String> values = Objects.requireNonNull(c.element()).getValue();
                        StringBuilder sql = new StringBuilder("INSERT INTO my_table (relationship_manager_id, name, team) VALUES ");
                        StringBuilder updateClause = new StringBuilder();

                        assert values != null;
                        for (String value : values) {
                            sql.append(value).append(",");
                        }
                        // Remove the last comma from the VALUES part
                        sql.setLength(sql.length() - 1);

                        // Construct the ON DUPLICATE KEY UPDATE clause
                        updateClause.append("name = VALUES(name), team = VALUES(team)");

                        sql.append(" ON DUPLICATE KEY UPDATE ").append(updateClause).append(";");

                        // Print the SQL query
                        System.out.println(sql);

                        // Output the SQL query
                        c.output(sql.toString());
                    }
                }));

        PipelineResult result = pipeline.run();
        // Wait for the pipeline to finish
        result.waitUntilFinish();
    }
}


