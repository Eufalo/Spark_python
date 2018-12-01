from pyspark import SparkConf, SparkContext
import sys


def loadHeroesNames():
    heroesNames = {}
    with open("Marvel-names.txt") as f:
        for line in f:
            fields = line.split('"')
            heroesNames[int(fields[0])] = fields[1]
    return heroesNames

conf = SparkConf().setMaster("local").setAppName("Heroes_Popular")
sc = SparkContext(conf = conf)

nameDict = sc.broadcast(loadHeroesNames())


lines = sc.textFile("Marvel-graph.txt")

heroesNumConcurrencia = lines.map(lambda x: (int(x.split()[0]), len(x.split())-1))#  Modificamos el RDD para que sea de tipo diccionario clave ID heroe, numero de heroes que aparece con el en la saga

heroesCounts = heroesNumConcurrencia.reduceByKey(lambda x, y: x + y) # Agrupamos por ID de personaje por si hubiera personajes repetidos

flipped = heroesCounts.map( lambda x : (x[1], x[0])) # Para poder ordenar por popularidad devemos hacer un flip entre la posicion ID y popularidad

sortedHeroes = flipped.sortByKey(ascending= False) # Ordenamos por popularidad de manera descendente 1 posicion mas popular

sortedHeroesWithNames = sortedHeroes.map(lambda countHeroes : (nameDict.value[countHeroes[1]], countHeroes[0])) # Trasformamos el id del personaje a su nombre para que sea mas sencillo interpretar los datos
lstPopuMarvel=sortedHeroesWithNames.collect() # Convertimos el objeto RRD Spark a una lista de python

numeroSuperheroes=int(sys.argv[1])
#print(lstPopuMarvel[:int(numeroSuperheroes)])
if(numeroSuperheroes<len(lstPopuMarvel) and (numeroSuperheroes>0)):
    print ( lstPopuMarvel[:numeroSuperheroes])
