GIANG Andre
YVOZ Stéphane
# README
## Projet de Big Data
#### Lancement du programme :
    Au lancement du programme, il vous est demandé de choisir quels documents (cf ou cp) doivent être utilisé, ainsi que le traitement qui doit leurs être appliqué (Question 1 à 4 ou questions 5 à 10).
    
#### Explication du code :
##### Q1)  
        On commence par charger tous les documents dans une JavaRDD.
##### Q2)
    Pour compter les mots, on split tous les String contenus dans la premiére RDD, puis on s'assure que tous les mots soient entiérement en minuscule (pour éviter d'avoir deux fois le même mot par la suiTe). On associe ensuite à chacun de ces mots l'entier 1 (avec mapToPair), pour ensuite utiliser reduceByKey afin d'obtenir le nombre d'apparition de chaque mot.
    
    On pense également à enlever toute occurence du caractére vide ("").
    
    Comme il est impossible de trier directement par valeur, on commence par faire un swap des clé et des valeurs, puis on les tri avec SortByKey, et on finit par les remettre dans le bon sens avec un autre swap.
    CPour calculer le nombre 
