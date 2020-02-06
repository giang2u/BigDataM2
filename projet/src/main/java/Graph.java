import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

import scala.Tuple2;

public class Graph {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("BigData").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
        SparkSession spark = SparkSession.builder().appName("BigData").getOrCreate();

        
        /*
         
        //Question 2
        //On crée les vertices (airport) et les edges (route)

        StructType airportSchema = new StructType().add("id", "int").add("Name", "string").add("City", "string").add("Country", "string").add("IATA", "string").add("ICAO", "string")
                .add("Latitude", "double").add("Longitude", "double").add("Altitude", "int").add("Timezone", "int")
                .add("DST", "string").add("Tz database time zone", "string").add("Type", "string").add("Source", "string");


        Dataset<Row> vertices = spark.read().option("mode", "DROPMALFORMED").schema(airportSchema).csv("src/main/resources/airports.dat");

        

        StructType routeSchema = new StructType().add("Airline", "string").add("airlineID", "int").add("sourceAirport", "string")
                .add("src", "int").add("destinationAirport", "string").add("dst", "int")
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
       
       // Q8 ----------------- 
       
       Dataset<Row> fusion = Indegree.join(Outdegree, Indegree.col("id").equalTo(Outdegree.col("id")) );
     
       
       //mot2.mapToPair( s -> s.swap() ).keys().reduce( (v1,v2) -> (v1+v2) );
       
       Dataset<Row> calcul = fusion.withColumn("yes", Indegree.col("inDegree").divide(Outdegree.col("outDegree") ) );
       
       calcul.show();
       
       
       
       // Q9 utiliser g.triangleCount().run()
       
       Dataset<Row> nbtriangle = g.triangleCount().run();
       nbtriangle.orderBy(functions.col("count").desc()).show();
       
       // Q10 --------------------------------------------
       
       System.out.println("BONJOUR " + g.vertices().count() );
       System.out.println("BONJOUR " + g.edges().count() );
       
       */
       
       // Q11 -----------------------------------------------
       
        StructType airportSchema = new StructType().add("id", "int").add("Name", "string").add("City", "string").add("Country", "string").add("IATA", "string").add("ICAO", "string")
                .add("Latitude", "double").add("Longitude", "double").add("Altitude", "int").add("Timezone", "int")
                .add("DST", "string").add("Tz database time zone", "string").add("Type", "string").add("Source", "string");


        Dataset<Row> verticesv2 = spark.read().option("mode", "DROPMALFORMED").schema(airportSchema).csv("src/main/resources/airports.dat");
        
      
        StructType routeSchemav2 = new StructType().add("src", "int").add("source", "string")
                .add("dst", "int").add("dest", "string").add("dep_delay", "double")
                .add("dep_delay_new", "double").add("dep_del15", "double");

        Dataset<Row> edgesv2 = spark.read().option("mode", "DROPMALFORMED").option("header", "true").schema(routeSchemav2).csv("src/main/resources/routesv2.csv");
        
        
        GraphFrame gv2 = new GraphFrame(verticesv2, edgesv2);
        
     
        // 1 ---------------------------
        
        //gv2.edges().filter( "source = 'SFO'" ).orderBy(functions.col("dep_delay").desc()).show();
        
        
        // 2 ------------------------------
        
        //gv2.edges().filter( "source = 'SEA'" ).filter(" dep_delay > 10").select("source","dest", "dep_delay").show();
        
        
        // Q12 ---------------------------
        
        Dataset<Row> trajetSJS = gv2.find("(SFO)-[]->(JAC); (JAC)-[]->(SEA)");
        //trajetSJS.filter("SFO.IATA = 'JAC'").show();
        //trajetSJS.show();
        //trajetSJS.distinct().show();
       // .filter(" JAC.IATA = 'JAC'").filter(" SEA.IATA = 'SEA'")
    }

}

