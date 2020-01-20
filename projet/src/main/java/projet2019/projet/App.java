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
    	 int i = 0;
    	 File dir = new File("EVC-TXT/cf");
    	 List<Row> data = new ArrayList<Row>();
    	 for (File f : dir.listFiles()) {
    		 
    		 System.out.println(i +  "   " + f.getPath());
    		 JavaRDD<String> stock = spark.read().textFile(f.getPath()).javaRDD();
    		 
    		 JavaRDD<String> item = stock.flatMap(s -> Arrays.asList(s.split("[^a-zA-ZáàâäãéèêëíìîïóòôöõúùûüýÿÁÀÂÄÃÉÈÊËÍÌÎÏÓÒÔÖÕÚÙÛÜÝ]")).iterator())
						 						.map(String::toLowerCase);
    		 
    		 List<String> listItem = item.collect();
    		 
    		 data.add( RowFactory.create(Arrays.asList( listItem ) ) ) ;
    		 i++;
    	 }
    	 /*
    	 for (Row r : data) {
    		 System.out.println(r.toString() );
    		 Thread.sleep(5000);
    	 }
    	 
    	 */
    	 
    	 // Q6) -------------------------
    }
}
