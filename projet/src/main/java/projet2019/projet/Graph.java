import org.apache.spark.SparkConf;
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

        //Question 2
        //On crée les vertices (airport) et les edges (route)

        StructType airportSchema = new StructType().add("ID", "int").add("Name", "string").add("City", "string").add("Country", "string").add("IATA", "string").add("ICAO", "string")
                .add("Latitude", "double").add("Longitude", "double").add("Altitude", "int").add("Timezone", "int")
                .add("DST", "string").add("Tz database time zone", "string").add("Type", "string").add("Source", "string");


        Dataset<Row> vertices = spark.read().option("mode", "DROPMALFORMED").schema(airportSchema).csv("src/main/resources/airports.dat");

        

        StructType routeSchema = new StructType().add("Airline", "string").add("airlineID", "int").add("src", "string")
                .add("sourceAirportID", "int").add("dst", "string").add("destinationAirportID", "int")
                .add("codeshare", "string").add("stops", "int").add("equipment", "string");

        Dataset<Row> edges = spark.read().option("mode", "DROPMALFORMED").schema(routeSchema).csv("src/main/resources/routes.dat");
        
        
        // Q3  ---------------------------
        GraphFrame g = new GraphFrame(vertices, edges);
        
        
        
        // Q4 ------------------
        g.edges().show();
        
        
        
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
       
       
       Dataset<Row> fusion = Indegree.join(Outdegree, Indegree.col("id").equalTo(Outdegree.col("id")) );
     
       
       //mot2.mapToPair( s -> s.swap() ).keys().reduce( (v1,v2) -> (v1+v2) );
       
       Dataset<Row> calcul = fusion.withColumn("yes", Indegree.col("inDegree").divide(Outdegree.col("outDegree") ) );
       
       calcul.show();
       
    }

}
