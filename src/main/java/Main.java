import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDD$;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.apache.zookeeper.data.Stat;

import java.util.*;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class Main {

    public static void main(String[] args) {

        System.out.println("Hello, World");

        SaveDataFrame();
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


    public static  void InnerJoin()
    {

        SparkConf conf = new SparkConf(true)
                .set("spark.cassandra.connection.host", "192.168.1.85");
        //.set("spark.cassandra.auth.username", "cloud")
        //.set("spark.cassandra.auth.password", "scape");
        conf.setAppName("Java API demo");
        conf.setMaster("local");
        //conf.set("spark.driver.allowMultipleContexts", "true");


        //JavaSparkContext sc = new JavaSparkContext(conf);
        // SQLContext sqlContext = new SQLContext(sc);



        SparkContext sparkContext = new SparkContext(conf);
        CassandraSQLContext csc = new CassandraSQLContext(sparkContext);
        csc.setKeyspace("analytics");
        org.apache.spark.sql.DataFrame dataFrame = csc.sql("SELECT page.url as page_url, cursor.url as cursor_url FROM page INNER JOIN cursor on cursor.url=page.url");
        long ff =  dataFrame.count();
    }
    public static  void SaveDataFrame()
    {

        SparkConf conf = new SparkConf(true);
        conf.setAppName("Java API demo");
        conf.setMaster("local");
        conf.set("spark.cassandra.connection.host", "192.168.1.85");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);




        DataFrame dataframe = sqlContext.read()
                .format("org.apache.spark.sql.cassandra")
                .options(ImmutableMap.of("table", "page", "keyspace", "analytics"))
                .load();
        /*
        for (Row row : dataframe.collect()){
            System.out.println(row);
        }
       */
         

    }
}
