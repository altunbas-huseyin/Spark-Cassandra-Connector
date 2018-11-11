import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.zookeeper.data.Stat;

import java.util.*;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class Main {

    public static void main(String[] args) {

        System.out.println("Hello, World");


        SparkConf conf = new SparkConf(true)
                .set("spark.cassandra.connection.host", "192.168.1.85");
        //.set("spark.cassandra.auth.username", "cloud")
        //.set("spark.cassandra.auth.password", "scape");
        conf.setAppName("Java API demo");
        conf.setMaster("local");


        JavaSparkContext sc = new JavaSparkContext(conf);



        JavaRDD<CassandraRow> rdd = javaFunctions(sc).cassandraTable("analytics", "page");

          /*
        for (CassandraRow row : rdd.collect()){
            System.out.println(row);
        }
*/

        long ff = rdd.count();
        sc.close();
        System.out.println(ff);




    }
}
