package org.example;

import com.google.cloud.bigquery.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import com.google.api.services.bigquery.model.TableRow;
//import org.example.curation.MyData;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class curation_c11 {
    public static void main(String[] args) {
        // Create and initialize pipeline options
        String[] arguments = {"--runner=DirectRunner", "--tempLocation=gs://cardnet_2", "--project=burner-mihparma"};
        PipelineOptions options = PipelineOptionsFactory.fromArgs(arguments).withValidation().as(PipelineOptions.class);

        options.setTempLocation("gs://cardnet_1");
        Pipeline pipeline = Pipeline.create(options);
        // Define the BigQuery source and sink tables
        String sourceTable = "burner-mihparma.cardnet_c11.cardnet_account_attributes";
        String sinkTable = "burner-mihparma:cardnet_c11.Output_1";

        String Final_Query = "SELECT * FROM " + sourceTable;
        //STEP 1: Read from BQ
        PCollection<TableRow> rows = pipeline.apply
                ("Read data from big query and apply simple labels", BigQueryIO.readTableRows()
                        .fromQuery(Final_Query)
                        .usingStandardSql());
        System.out.println("*******");

        //STEP 2: Apply ParDo to process each element
        PCollection<TableRow> output = rows.apply("ProcessRows", ParDo.of(new ProcessRowFn()));

        // ========================================================================================
        // Now Checking for Duplicates in staging table to handle update and append operations
        // Read Stage layer (Old table) table from BQ:
        PCollection<TableRow> stage_table = pipeline.apply
                ("Read data from big query and apply simple labels", BigQueryIO.readTableRows()
                        .fromQuery("SELECT * FROM burner-mihparma.cardnet_c11.Output_1")
                        .usingStandardSql());

        PCollectionView<List<TableRow>> stage_view = stage_table.apply(View.asList());

        final TupleTag<TableRow> existingMIDS = new TupleTag<>(){};
        final TupleTag<TableRow> newMIDS = new TupleTag<>(){};

        PCollectionTuple results = output.apply("Compare PCollections", ParDo.of(new DoFn<TableRow, TableRow>() {

            @ProcessElement
            public void processElement(ProcessContext c) {
                List<TableRow> stage_rows = c.sideInput(stage_view);
                TableRow tableRow = checkIfAlreadyPresent(stage_rows, c.element());
                if(tableRow != null){
                    System.out.println("Matched ####");
                    c.output(existingMIDS,tableRow);
                }
                else {
                    System.out.println("##### Not matched ");
                    c.output(c.element());
                }
            }
            public TableRow checkIfAlreadyPresent(List<TableRow> accounts, TableRow row){
                if(accounts.stream().anyMatch(o -> (o.get("Internal_MID").equals(row.get("Internal_MID")) )))
                {
                    TableRow record = accounts.stream().filter(o -> (o.get("Internal_MID").equals(row.get("Internal_MID")))).findFirst().get();
                    TableRow tableRow = new TableRow();

                    tableRow.set("Internal_MID", record.get("Internal_MID"));
                    tableRow.set("Parent_Internal_MID", record.get("Parent_Internal_MID"));
                    tableRow.set("_Merchant_Internal_MID", record.get("_Merchant_Internal_MID"));
                    tableRow.set("Account_Type", record.get("Account_Type"));
                    tableRow.set("Settlement_sort_code", record.get("Settlement_sort_code"));
                    tableRow.set("LBG_Settlement", record.get("LBG_Settlement"));
                    return tableRow;
                }
                return null;
            }
        }).withSideInputs(stage_view).withOutputTags(newMIDS,TupleTagList.of(existingMIDS)));

        PCollection<TableRow> new_mids = results.get(newMIDS);
        new_mids.apply("WriteToBigQuery", BigQueryIO.writeTableRows()
                .to(sinkTable)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        PCollection<TableRow> existing_mids = results.get(existingMIDS);
        existing_mids.apply("Update BQ", ParDo.of(new UpdateBQ()));
        //=========================================================================================

        //STEP 3 : Write output table:
        //Write the transformed data to the sink BigQuery table
//        output.apply("WriteToBigQuery", BigQueryIO.writeTableRows()
//                .to(sinkTable)
//                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
//                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
//        System.out.println("1232****");
        // Run the pipeline
        pipeline.run();
    }

    // Define a DoFn to process each TableRow and set the output based on col1 and col2
    public static class ProcessRowFn extends DoFn<TableRow, TableRow> {
        @ProcessElement
        public void processElement(@Element TableRow row, OutputReceiver<TableRow> out) {
            String Settlement_sort_code = (String) row.get("Settlement_sort_code");
            Integer[] lbg_numbers = {30, 77, 12, 80};
            // Taking out first two numbers from Settlement_sort_code.
            TableRow newRow = new TableRow();
            newRow.putAll(row);
            if (Settlement_sort_code != null) {
                int firstTwoDigits = Integer.parseInt(Settlement_sort_code.substring(0, 2));
                System.out.println(firstTwoDigits);
                if(Arrays.asList(lbg_numbers).contains(firstTwoDigits)){
                    newRow.set("LBG_Settlement", "TRUE");
                }
                else {
                    newRow.set("LBG_Settlement", "FALSE");
                }
            }
            else {
                newRow.set("LBG_Settlement", "FALSE");
            }

            out.output(newRow);
        }
    }

    public static class UpdateBQ extends DoFn<TableRow, Void> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {


            BigQuery bigQuery = BigQueryOptions.newBuilder().setProjectId("burner-mihparma").build().getService();

            String update_query = ("UPDATE cardnet_c11.Output_1 SET " +
                    "Parent_Internal_MID={Parent_Internal_MID}, " +
                    "_Merchant_Internal_MID={_Merchant_Internal_MID}, " +
                    "Account_Type=\"{Account_Type}\", " +
                    "Settlement_sort_code=\"{Settlement_sort_code}\", " +
                    "LBG_Settlement=\"{LBG_Settlement}\", " +
                    "Internal_MID=\"{Internal_MID}\" " +
                    "WHERE Internal_MID=\"{Internal_MID}\"")
                    .replace("{Parent_Internal_MID}",c.element().get("Parent_Internal_MID") != null ? c.element().get("Parent_Internal_MID").toString() : "NULL")
                    .replace("{_Merchant_Internal_MID}", c.element().get("_Merchant_Internal_MID") != null ? c.element().get("_Merchant_Internal_MID").toString() : "NULL")
                    .replace("{Account_Type}",c.element().get("Account_Type") != null ? c.element().get("Account_Type").toString() : "NULL")
                    .replace("{Settlement_sort_code}", c.element().get("Settlement_sort_code") != null ? c.element().get("Settlement_sort_code").toString() : "NULL")
                    .replace("{LBG_Settlement}",c.element().get("LBG_Settlement") != null ? c.element().get("LBG_Settlement").toString() : "NULL")
                    .replace("{Internal_MID}", c.element().get("Internal_MID") != null ? c.element().get("Internal_MID").toString() : "NULL");

            System.out.println(update_query);
            update_query = update_query.replace("\"NULL\"","NULL");
            System.out.println("============================");
            System.out.println(update_query);
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(update_query).build();

            Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).build());
            queryJob = queryJob.waitFor();

            if (queryJob.getStatus().getError() != null) {
                throw new Exception(queryJob.getStatus().getError().toString());

            }
        }
    }
}

