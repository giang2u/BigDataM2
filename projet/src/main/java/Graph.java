import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

public class Graph {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("BigData").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
        SparkSession spark = SparkSession.builder().appName("BigData").getOrCreate();

        spark.sparkContext().setLogLevel("Error");
        //Question 2
        //On crée les vertices (airport) et les edges (route)

        StructType airportSchema = new StructType().add("id", "int").add("Name", "string").add("City", "string").add("Country", "string").add("IATA", "string").add("ICAO", "string")
                .add("Latitude", "double").add("Longitude", "double").add("Altitude", "int").add("Timezone", "int")
                .add("DST", "string").add("Tz database time zone", "string").add("Type", "string").add("Source", "string");


        Dataset<Row> vertices = spark.read().option("mode", "DROPMALFORMED").schema(airportSchema).csv("EVC-TXT/airports.dat");

        

        StructType routeSchema = new StructType().add("Airline", "string").add("airlineID", "int").add("sourceAirport", "string")
                .add("src", "int").add("destinationAirport", "string").add("dst", "int")
                .add("codeshare", "string").add("stops", "int").add("equipment", "string");

        Dataset<Row> edges = spark.read().option("mode", "DROPMALFORMED").schema(routeSchema).csv("EVC-TXT/routes.dat");
        
        
        // Q3  ---------------------------
        GraphFrame g = new GraphFrame(vertices, edges);
        
        
        
        // Q4 ------------------
        g.edges().show();
        
        
      /*  
        // Q5 ----------------------
        
       Dataset<Row> degree = g.degrees().orderBy(functions.col("degree").desc());
       degree.persist();
       degree.show();
        
        
       // Q6 -------------------------
       Dataset<Row> Indegree = g.inDegrees().orderBy(functions.col("InDegree").desc());
       Indegree.persist();
       Indegree.show();
       
       // Q7 ------------------------------------------------
       Dataset<Row> Outdegree = g.outDegrees().orderBy(functions.col("OutDegree").desc());
       Outdegree.persist();
       Outdegree.show();
       
       
       
       
       degree.show();
       Indegree.show();
       Outdegree.show();
       
       
       Dataset<Row> fusion = Indegree.join(Outdegree, "id");
       
       Dataset<Row> calcul = fusion.withColumn("result", Indegree.col("inDegree").divide(Outdegree.col("outDegree")) )
    		   .select("id","result").orderBy(functions.col("result").desc());
       
       
       calcul.show();
       
       // Q9 utiliser g.triangleCount().run()
       */
       Dataset<Row> nbtriangle = g.triangleCount().run();
       nbtriangle.orderBy(functions.col("count").desc()).show();
       
       
    }

}
