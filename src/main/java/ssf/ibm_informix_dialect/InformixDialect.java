package ssf.ibm_informix_dialect;

import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;

import scala.Option;

/**
 * Creation of a dialect for communicating Spark with IBM Informix
 * 
 * @author ssf
 */
public class InformixDialect extends JdbcDialect {
  private static final long serialVersionUID = -123456;

  @Override
  public boolean canHandle(String url) {
    return url.startsWith("jdbc:informix");
  }

  /**
   * Function in charge of transforming JDBC types to Catalyst types
   */
  @Override
  public Option<DataType> getCatalystType(int sqlType,
      String typeName, int size, MetadataBuilder md) {
    if (typeName.toLowerCase().compareTo("serial") == 0) {
      return Option.apply(DataTypes.IntegerType);
    }
    if (typeName.toLowerCase().compareTo(
        "se_metadata") == 0) {
      return Option.apply(DataTypes.BinaryType);
    }
    if (typeName.toLowerCase().compareTo(
        "sysbldsqltext") == 0) {
      return Option.apply(DataTypes.BinaryType);
    }
    if (typeName.toLowerCase().startsWith("timeseries")) {
      return Option.apply(DataTypes.BinaryType);
    }
    if (typeName.toLowerCase().compareTo(
        "tspartitiondesc_t") == 0) {
      return Option.apply(DataTypes.BinaryType);
    }
    if (typeName.toLowerCase().compareTo("st_point") == 0) {
      return Option.apply(DataTypes.BinaryType);
    }    
    if (typeName.toLowerCase().compareTo("calendar") == 0) {
      return Option.apply(DataTypes.BinaryType);
    }
    if (typeName.toLowerCase().compareTo(
        "calendarpattern") == 0) {
      return Option.apply(DataTypes.BinaryType);
    }

    return Option.empty();
  }
}
