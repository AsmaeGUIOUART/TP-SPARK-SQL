package ma.enset.tp1sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class Application2 {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession ss=SparkSession.builder()
                .appName("Spark SQL App")
                .master("local[*]")
                .getOrCreate();

        Encoder<Employe> employeEncoder=Encoders.bean(Employe.class);
        Dataset<Employe> ds1=ss.read().option("multiline",true).json("employes.json").as(employeEncoder);
        ds1.filter((FilterFunction<Employe>) emp->emp.getName().startsWith("A")).show();
        Dataset<String> ds2=ss.createDataset(
                Arrays.asList("Ahmed","Karim"),
                Encoders.STRING()
        );
       ds2.show();
    }
}
