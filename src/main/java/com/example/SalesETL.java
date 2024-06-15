package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.values.PCollection;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

public class SalesETL {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pipeline = Pipeline.create(options);

    String dbUrl = System.getenv("POSTGRES_URL");
    String dbUser = System.getenv("POSTGRES_NAME");
    String dbPassword = System.getenv("POSTGRES_PW");

    // 1. Extract CSV Data
    PCollection<String> lines = pipeline
        .apply("Read CSV", TextIO.read().from(System.getenv("SALES_CSV")));

    // 2. Transform and Filter
    PCollection<Sale> filteredSales = lines
        .apply("Parse and Transform", ParDo.of(new DoFn<String, Sale>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            String[] fields = c.element().split(","); // Assuming comma-separated CSV
            // Skip the header
            if (fields[0].equalsIgnoreCase("product_id")) {
              return;
            }
            try {
              int quantity = Integer.parseInt(fields[2]);
              if (quantity > 0) { // Quantity is the third column (index 2)
                Sale sale = new Sale();
                sale.setProductId(fields[0]);
                sale.setCustomerId(fields[1]);
                sale.setQuantity(quantity);
                sale.setSaleDate(LocalDate.now());
                c.output(sale);
              }
            } catch (Exception e) {
              // Handle parsing exceptions if needed
              e.printStackTrace();
            }
          }
        }))
        .apply("Filter Zero Quantity", Filter.by(sale -> sale.getQuantity() > 0));

    // 3. Load into PostgreSQL
    filteredSales.apply("Write to PostgreSQL", JdbcIO.<Sale>write()
        .withDataSourceConfiguration(
            JdbcIO.DataSourceConfiguration.create("org.postgresql.Driver", dbUrl)
                .withUsername(dbUser)
                .withPassword(dbPassword))
        .withStatement("INSERT INTO sales (product_id, customer_id, quantity, sale_date) VALUES (?, ?, ?, ?)")
        .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<Sale>() {
          public void setParameters(Sale sale, PreparedStatement query) throws SQLException {
            query.setString(1, sale.getProductId());
            query.setString(2, sale.getCustomerId());
            query.setInt(3, sale.getQuantity());
            query.setDate(4, java.sql.Date.valueOf(sale.getSaleDate()));
          }
        }));

    // 4. Execute Pipeline
    pipeline.run().waitUntilFinish();
  }
}
