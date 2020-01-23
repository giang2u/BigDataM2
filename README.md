GIANG Andre
YVOZ Stéphane
# README
## Projet de Big Data
#### Lancement du programme :
Au lancement du programme, il vous est demandé de choisir quels documents (cf ou cp) doivent être utilisé, ainsi que le traitement qui doit leurs être appliqué (Question 1 à 4 ou questions 5 à 10). Si on souhaite appliquer le traitement des questions 5 à 10, il est également demandé de donner le minsup, minconf et le nombre k de résultats à afficher.
    
#### Explication du code :
##### Q1)  
On commence par charger tous les documents dans une JavaRDD.
##### Q2)
Pour compter les mots, on split tous les String contenus dans la premiére RDD, puis on s'assure que tous les mots soient entièrement en minuscule (pour éviter d'avoir deux fois le même mot par la suite). On associe ensuite à chacun de ces mots l'entier 1 (avec mapToPair), pour ensuite utiliser reduceByKey afin d'obtenir le nombre d'apparition de chaque mot.
    
On pense également à enlever toute occurence du caractére vide ("").
    
Comme il est impossible de trier directement par valeur, on commence par faire un swap des clé et des valeurs, puis on les tri avec SortByKey, et on finit par les remettre dans le bon sens avec un autre swap.
    
##### Q3)
    
Pour filter les mots contenus dans stopwords, on commence par les charger dans une RDD, pour ensuite utiliser collect() pour les stocker dans une liste de String.
On applique ensuite un filtre aux mots des documents avec la fonction filter(), en ne gardant que les mots non contenus dans la liste des stopwords.
`JavaPairRDD<String, Integer> motFiltrer = motTrie.filter(word -> !listStopWord.contains(word._1));`
    
#### Q4)

Comme le tri à déja été effectué dans la question 2 et que le filtre ne change pas l'ordre des éléments de la RDD contenant les mots des documents, il ne nous reste qu'a récupérer les 10 premiers mots avec take(10).
    
#### Q5)

On stocke les transactions dans une liste de Row. Pour remplir cette liste, on récupére chaque fichier individuellement, on transforme ce document en une liste de String. Cette liste de String sera débarassée des doublons en utilisant :
   `List<String> listItem = itemFiltrer.collect().stream().distinct().collect(Collectors.toList());`
On converti ensuite cette liste de String en Row grace à une RowFactory.

#### Q6) 
Pour filtrer en utilisant les stopwords, on commence par charger la liste de stopwords, puis on effectue le même filtre que dans la question 3, mais sur chaque transaction individuellement lors du chargement de celle ci (avant sa transormation en Row) 

#### Q7)
Pour appliquer l'algorithme FPGrowth, il est nécessaire de charger la librairie mllib en modifiant le fichier pom.xml:
<dependency> 
      <groupId>org.apache.spark</groupId> 
      <artifactId>spark-mllib_2.11</artifactId> 
      <version>2.3.0</version> 
     </dependency> 

Ensuite, on crée un Dataset de Row à partir d'un schema et de la liste de Row contenant les transaction et créée à la question 5.
Nous pouvons maintenant appliquer FPGrowth sur ce dataset avec : 
`PGrowthModel fpg = new FPGrowth().setItemsCol("items").setMinSupport(minSup).fit(datas);`

#### Q8) 
Pour trier les résultats , on utilise la fonction OrderBy:
`fpg.freqItemsets().orderBy(functions.col("freq").desc())`

On remarque alors que suivant la valeur de Minsup, le tri peut prendre trés lomgtemps:
avec minsup = 0,8, le tri est quasi instantanée
avec minsup = 0,55, le tri prend 143 secondes,
avec minsup= 0,5 et en dessous, le tri ne se fini pas en temps raisonnable.

#### Q9)
Pour varier le minConf, on modifie l'apell à FPGrowth en ajoutant .setMinConfidence(minConf) :
 `FPGrowthModel fpg = new FPGrowth().setItemsCol("items").setMinSupport(minSup).setMinConfidence(minConf).fit(datas);`

#### Q10)
Le tri des résultats se passe comme dans la question 8.
Nous n'avons pas remarqué de changements significatifs en variant minConf par rapport à la question 8.

#### Q11)
Pour choisir le dossier cp , il suffit de le préciser au lancement du programme.


