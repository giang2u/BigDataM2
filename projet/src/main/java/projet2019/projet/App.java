package projet2019.projet;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	
    	
    	 SparkSession spark = SparkSession
    		      .builder()
    		      .appName("App")
    		      .config("spark.master", "local")
    		      .getOrCreate();

    	 // recupere toutes les lignes des fichiers txt dans cf
    	 JavaRDD<String> lines = spark.read().textFile("EVC-TXT/cf/*.txt").javaRDD();
    	 
    	 

     	 
    	 // separe tous les mots qui ont un espace et les stocke 
    	 JavaPairRDD<String, Integer> mot = lines.flatMap(s -> Arrays.asList(s.split("[^a-zA-ZáàâäãéèêëíìîïóòôöõúùûüýÿÁÀÂÄÃÉÈÊËÍÌÎÏÓÒÔÖÕÚÙÛÜÝ]")).iterator())
    			 								 .mapToPair(word -> new Tuple2<>(word,1))
    			 								 .reduceByKey((v1,v2) -> (v1 + v2));
    	 
    	 
    	 
    	 System.out.println(mot.collect());
    	
    	 
    	 //System.out.println(lines.take(2));
    		    
    		    
        System.out.println( "Hello World!" );
    }
}
