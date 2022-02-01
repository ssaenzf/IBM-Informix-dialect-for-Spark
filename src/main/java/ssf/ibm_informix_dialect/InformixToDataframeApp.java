package ssf.ibm_informix_dialect;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;

/**
 * A class for ingesting from IBM Informix database
 * 
 * @author ssf
 */
public class InformixToDataframeApp {

  /**
   * Function for starting the application
   * 
   * @param args
   */
  public static void main(String[] args) {
    InformixToDataframeApp app = new InformixToDataframeApp();
    app.start();
  }

  /**
   * Function for reading Informix database
   */
  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName(
            "Informix to Dataframe using a JDBC Connection")
        .master("local")
        .getOrCreate();

    JdbcDialect dialect = new InformixDialect();
    JdbcDialects.registerDialect(dialect);

    Dataset<Row> df = spark
        .read()
        .format("jdbc")
        .option(
            "url",
            "jdbc:informix-sqli://[::1]:33378/stores_demo:IFXHOST=lo_informix1210;DELIMIDENT=Y")
        .option("dbtable", "customer")
        .option("user", "informix")
        .option("password", System.getenv("DB_PASSWORD")) // Database password for Informix account
        .load();

    // Displays the scheme and some data of the database
    df.show(10);
    df.printSchema();
  }
}
