package ma.enset.tp1sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class Application1 {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession ss=SparkSession.builder()
                .appName("Spark SQL App")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df1=ss.read().option("multiline",true).json("employes.json");
        Dataset<Row> df2= df1.select(col("name"),col("salary").plus(2000));
        //df2.show();
        //<Row> df3=df1.filter(col("salary").gt(20000));
        Dataset<Row> df3=df1.filter("salary>20000");
       // df3.show();
        Dataset<Row> df4=df1.groupBy("department").count();
        //df4.show();
        df1.createOrReplaceTempView("employes");
        ss.sql("select * from employes where name like 'A%'").show();
    }
}

