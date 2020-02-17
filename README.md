GIANG Andre
YVOZ Stéphane
# README
## Projet de Big Data

#### Explication du code :

##### Q2)
Pour créer les Vertices, on commence par définir un schéma selon lequel les données dans airports.dat sont organisées.
`StructType airportSchema = new StructType().add("id", "int").add("Name", "string").add("City", "string").add("Country", "string").add("IATA", "string").add("ICAO", "string")
                .add("Latitude", "double").add("Longitude", "double").add("Altitude", "int").add("Timezone", "int")
                .add("DST", "string").add("Tz database time zone", "string").add("Type", "string").add("Source", "string");
 `
On va ensuite importer airports.dat en utilisant ce schéma et en traitant le fichier comme un csv.
`Dataset<Row> vertices = spark.read().option("mode","DROPMALFORMED").schema(airportSchema).csv("src/main/resources/airports.dat");`

Les routes sont elles aussi importées de la même façon (schéma puis import en tant que csv)

##### Q3)
Il suffit d'instancier un objet de la classe GraphFrame avec les aéroport en tant que vertices et les routes en tant que edges.
    `GraphFrame g = new GraphFrame(vertices, edges);`

##### Q4)
Pour accéder aux edges, il faut utiliser la méthode edges() de GraphFrames, puis la méthode show().
 (Insérer code ici)
`
##### Q5)
Pour afficher et trier le degré de chaque vertex, il suffit d'utiliser las méthodes degrees() et OrderBy()
`Dataset<Row> degree = g.degrees().orderBy(functions.col("degree").desc());`

##### Q6)
Il suffit de remplacer la méthode degrees() de la question précédente par la méthode inDegrees().
`Dataset<Row> Indegree = g.inDegrees().orderBy(functions.col("InDegree").desc());`

##### Q7)
Il faut remplacer la méthode degrees() de la question 6 par la méthode outDegrees().
`Dataset<Row> Outdegree = g.outDegrees().orderBy(functions.col("OutDegree").desc());`
##### Q8)
Pour calculer les aéroports avec les meilleurs ratios de transferts, nous commencont par créer un dataset contenant à la fois les inDegrees et les outDegrees :
` Dataset<Row> fusion = Indegree.join(Outdegree, Indegree.col("id").equalTo(Outdegree.col("id")) );`
Puis nous créons un autre Dataset grace à une opération de fusion des colonnes de degrees, en divisant les inDegrees par les outDegrees:
`Dataset<Row> calcul = fusion.withColumn("yes",Indegree.col("inDegree").divide(Outdegree.col("outDegree") ) );`
Ensuite, comme nous souhaitons obtenir les aéroports avec les meilleurs ratios, c'est à dire avec les ratios les plus proches de 1, nous avons crée un dataset avec une colonne contenant la distance à 1:
`Dataset<Row> calcul2 = calcul.withColumn("dist", functions.abs(( calcul.col("ratio").minus(1) )) );`
Il ne reste ensuite qu'a trier ce dataset par ordre croissant sur la colonne des distance à 1:
`calcul2.orderBy( functions.col("dist").asc()).show();`

##### Q9)
Les triplets du graphe se calculent grace à la fonction triangleCount():
`Dataset<Row> nbtriangle = g.triangleCount().run();`

##### Q10)
Le nombre d'aéroports est par construction égal au nombre de vertices.
Le nombre de trajets est par construction égal au nombre de edges.
Ainsi, pour compter et afficher le nombre d'aéroports et le nombre de trajets, on utilise:
`System.out.println("Nombre d'aéroports " + g.vertices().count() );
       System.out.println("Nombre de trajets " + g.edges().count() );`

##### Q11)
###### 1)
On commence par appliquer un filtre afin de ne garder que les vols partants de SFO, puis on les tri des le sens decroissant des délais:
`gv2.edges().filter( "source = 'SFO'" ).orderBy(functions.col("dep_delay").desc()).show();`
###### 2)
On commencepar filtrer les vols afin de ne garder que ceux qui partent de SEA, puis on applique un deuxiéme filtre afin de ne garder que ceux avec un dep_delay > 10.
`gv2.edges().filter( "source = 'SEA'" ).filter(" dep_delay > 10").select("source","dest", "dep_delay").show();`

##### Q12)
###### 1)
La requéte pour trouver tous les trajets possibles afin d'aller de SFO à SEA en passanr par JAC devrait normalement se faire de la façon suivante :
`Dataset<Row> trajetSJS = gv2.find("(SFO)-[a1]->(JAC) ; (JAC)-[a2]->(SEA)").filter("SFO.IATA ='SFO'").filter("JAC.IATA ='JAC'").filter("SEA.IATA ='SEA'");`
Cependant, cette méthode ne fonctionne pas car la correspondance id/IATA des aéroports n'est pas la même dans aeroports.dat et dans routesv2.csv, ce qui empéche spark d'effectuer les recherches de motifs correctement.
A la place, nous avons extrait les vols allant de SFO à JAC et ceux allant de JAC à SEA:
`Dataset<Row> sfoAndSea = gv2.edges().filter("(source = 'SFO' AND dest = 'JAC') OR (source='JAC' AND dest='SEA') ");`
###### 2)
Il suffit d'appliquer un filtre au résultat précédent:
`sfoAndSea.filter("dep_delay < -5").show(false);

###### 3)
Pour obtenir la liste des destinations à partir de SFO, nous avons appliqué deux filtres afin de ne garder que les destinations des vols partant de SFO, puis nous avons utilisé la fonction distint() afin de retirer les doublons.
`Dataset <Row>destDepuisSfo = gv2.edges().filter("source = 'SFO'").select("dest").distinct();`

`
