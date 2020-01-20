package projet2019.projet;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec._;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws InterruptedException
    {
    	
    	
    	 SparkSession spark = SparkSession
    		      .builder()
    		      .appName("App")
    		      .config("spark.master", "local")
    		      .getOrCreate();

    	 
    	 // -------------------Q1)----------------------------
    	 

    	 Scanner sc = new Scanner(System.in);
    	 String rep = " ";
    	 do{ 
    		 System.out.println("Veuillez saisir cf ou cp : ");
    		 rep= sc.nextLine();
    		 System.out.println(rep);
    	 }
    	 while(!rep.equals("cf") && !rep.equals("cp") ) ;
    	
		 JavaRDD<String> lines = spark.read().textFile("EVC-TXT/"+rep+"/*.txt").javaRDD();


     	 
    	 // separe tous les mots qui ont un espace et les stocke 
    	 JavaPairRDD<String, Integer> mot = lines.flatMap(s -> Arrays.asList(s.split("[^a-zA-ZáàâäãéèêëíìîïóòôöõúùûüýÿÁÀÂÄÃÉÈÊËÍÌÎÏÓÒÔÖÕÚÙÛÜÝ]")).iterator())
    			 								 .map(String::toLowerCase)
    			 								 .mapToPair(word -> new Tuple2<>(word,1))
    			 								 .reduceByKey((v1,v2) -> (v1 + v2));
    	 
    	 
    	 //------- Q2)------------------------------
    	 
    	 long nbDiff = mot.count();
    	 
    	 // enleve les caracteres vides 
    	 JavaPairRDD<String, Integer> mot2 = mot.filter( word -> !word._1.equals(""));
    	 
    	 // echange cle value et trie par occurence puis les remet comme avant
    	 JavaPairRDD<String, Integer> motTrie = mot2.mapToPair(s -> s.swap()).sortByKey(false).mapToPair(s -> s.swap());
    	 
    	 // calcul le nombre total de mots
    	 Integer nbTotal = mot2.mapToPair( s -> s.swap() ).keys().reduce( (v1,v2) -> (v1+v2) );


    	 System.out.println(motTrie.collect());
    	 System.out.println("Nombre de mots differents " + nbDiff);
    	 System.out.println("Nombre de mots total " + nbTotal);
    	 
    	 
    	 
    	 // Q3) ---------------------------------------
    	// recupere toutes les lignes des fichiers french-stop
    	 JavaRDD<String> lines2 = spark.read().textFile("EVC-TXT/french-stopwords.txt").javaRDD();
    	 List<String> listStopWord = lines2.collect();
    	 
    	 
    	 // filtre : enleve tous les mots de lines2 dans motTrie
    	 JavaPairRDD<String, Integer> motFiltrer = motTrie.filter(word -> !listStopWord.contains(word._1));
    	
    	
    	 // Q4) --------------------------------------------
    	 // TOP TEN BEST WORDS
    	 System.out.println(motFiltrer.take(10));
    	 
    	 
    	 // Q5) ------------------------------------------------
    	 
    	 
    	 
    	 File dir = new File("EVC-TXT/cf");
    	 List<Row> data = new ArrayList<Row>();
    	 for (File f : dir.listFiles()) {
    		 
    		 JavaRDD<String> stock = spark.read().textFile(f.getPath()).javaRDD();
    		 JavaRDD<String> item = stock.flatMap(s -> Arrays.asList(s.split("[^a-zA-ZáàâäãéèêëíìîïóòôöõúùûüýÿÁÀÂÄÃÉÈÊËÍÌÎÏÓÒÔÖÕÚÙÛÜÝ]")).iterator())
						 						.map(String::toLowerCase);
    		 
    		 
    		 // enelevage des caractere vide
    		 JavaRDD<String> item2 = item.filter( word -> !word.equals(""));
    		 // Q6) ----------------------------------

        	 // filtre : enleve tous les mots de stopwords dans chaque transaction
        	 JavaRDD<String> itemFiltrer = item2.filter(word -> !listStopWord.contains(word));
        	 

    		 List<String> listItem = itemFiltrer.collect().stream().distinct().collect(Collectors.toList());
    		 
        	 
    		 data.add( RowFactory.create( listItem ) ) ;
    	 }
    	 
    	 
    	 
    	 // Q7) -------------------------
    	 
    	 StructType schema = new StructType(new StructField[] {new StructField("items", new ArrayType(DataTypes.StringType, true), false,Metadata.empty() ) } );
    	 
    	 Dataset<Row> datas = spark.createDataFrame(data, schema);
    	 
    	 
    	 FPGrowthModel fpg = new FPGrowth().setItemsCol("items").setMinSupport(0.2).fit(datas);
    	 fpg.freqItemsets().orderBy(functions.col("freq").desc()).show(false);
    	 
    	 
    	 
    		    
        System.out.println( "Hello World!" );
    }
}

