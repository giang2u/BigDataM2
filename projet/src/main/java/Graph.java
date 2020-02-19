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
        spark.sparkContext().setLogLevel("ERROR");
        
        
         
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
     
       
       Dataset<Row> calcul = fusion.withColumn("ratio", Indegree.col("inDegree").divide(Outdegree.col("outDegree") ) );
       
       Dataset<Row> calcul2 = calcul.withColumn("dist", functions.abs(( calcul.col("ratio").minus(1) )) );
       
       calcul2.orderBy( functions.col("dist").asc()).show();
       
       
       
       
       // Q9 
       
       Dataset<Row> nbtriangle = g.triangleCount().run();
       nbtriangle.orderBy(functions.col("count").desc()).show();
       
       // Q10 --------------------------------------------
       
       System.out.println("Nombre d'aéroports " + g.vertices().count() );
       System.out.println("Nombre de trajets " + g.edges().count() );
       
       
       
       // Q11 -----------------------------------------------
       
        StructType airportSchema2 = new StructType().add("id", "int").add("Name", "string").add("City", "string").add("Country", "string").add("IATA", "string").add("ICAO", "string")
                .add("Latitude", "double").add("Longitude", "double").add("Altitude", "int").add("Timezone", "int")
                .add("DST", "string").add("Tz database time zone", "string").add("Type", "string").add("Source", "string");


        Dataset<Row> verticesv2 = spark.read().option("mode", "DROPMALFORMED").schema(airportSchema2).csv("EVC-TXT/airports.dat");
        
      
        StructType routeSchemav2 = new StructType().add("src", "int").add("source", "string")
                .add("dst", "int").add("dest", "string").add("dep_delay", "double")
                .add("dep_delay_new", "double").add("dep_del15", "double");

       // Dataset<Row> edgesv2 = spark.read().option("mode", "DROPMALFORMED").option("header", "true").schema(routeSchemav2).csv("EVC-TXT/routesv2.csv");
        Dataset<Row> edgesv2 = spark.read().format("CSV").option("header", "true").schema(routeSchemav2).load("EVC-TXT/routesv2.csv");
        
        GraphFrame gv2 = new GraphFrame(verticesv2, edgesv2);
        
     
        // 1 ---------------------------
        
        gv2.edges().filter( "source = 'SFO'" ).orderBy(functions.col("dep_delay").desc()).show();
        
        
        // 2 ------------------------------
        
        gv2.edges().filter( "source = 'SEA'" ).filter(" dep_delay > 10").select("source","dest", "dep_delay").show();
        
        
        // Q12 ---------------------------
   
        Dataset<Row> sfoAndSea = gv2.edges().filter("(source = 'SFO' AND dest = 'JAC') OR (source='JAC' AND dest='SEA') ");
        sfoAndSea.show(false);
        
        //2 -----------------------
        
        sfoAndSea.filter("dep_delay < -5").show(false);
        
        //3-----------------
        
        Dataset <Row>destDepuisSfo = gv2.edges().filter("source = 'SFO'").select("dest").distinct();
        destDepuisSfo.show(false);
        
        
        //Dataset<Row> trajetSJS = gv2.find("(SFO)-[a1]->(JAC) ; (JAC)-[a2]->(SEA)").filter("SFO.IATA ='SFO'");
        //System.out.println(trajetSJS.count());
        //trajetSJS.filter("SFO.IATA = 'JAC'").show();
        //trajetSJS.show();
        //trajetSJS.distinct().show();
       // .filter(" JAC.IATA = 'JAC'").filter(" SEA.IATA = 'SEA'")
    }

}

