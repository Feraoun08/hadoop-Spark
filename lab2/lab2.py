from __future__ import print_function

from pyspark import SparkConf, SparkContext


conf = SparkConf().setAppName("TP2-Arbres")
sc = SparkContext(conf=conf)

FICHIER = "file:///root/arbres.csv"
SEP = ";"

# RDD 
lignes = sc.textFile(FICHIER)
print(" Nombre de lignes du RDD :", lignes.count())

# Séparer l'en-tête 
entete = lignes.first()
arbres = lignes.filter(lambda l: l != entete)
print("    Nombre d'arbres (sans l'en-tête) :", arbres.count())

# Trouver les colonnes à partir de l'en-tête
cols = [c.strip().upper() for c in entete.split(SEP)]
IDX_GENRE = next(i for i, c in enumerate(cols) if "GENRE" in c)
IDX_HAUTEUR = next(i for i, c in enumerate(cols) if "HAUTEUR" in c)

# Transformer chaque ligne en (genre, hauteur)
def extraire(ligne):
    c = ligne.split(SEP)
    try:
        return (c[IDX_GENRE].strip(), float(c[IDX_HAUTEUR]))
    except (IndexError, ValueError):
        return None

genre_hauteur = arbres.map(extraire).filter(lambda x: x is not None)
genre_hauteur.cache()

# Hauteur moyenne 
hauteurs = genre_hauteur.map(lambda p: p[1])
somme = hauteurs.reduce(lambda a, b: a + b)
nombre = hauteurs.count()
print(" Hauteur moyenne :", somme / nombre)

# Genre du plus grand arbre 
hauteur_genre = genre_hauteur.map(lambda p: (p[1], p[0]))
plus_grand = hauteur_genre.sortByKey(False).first()
print(" Genre du plus grand arbre :", plus_grand[1], "(hauteur =", plus_grand[0], ")")

# Nombre d'arbres par genre 
par_genre = genre_hauteur.map(lambda p: (p[0], 1)).reduceByKey(lambda a, b: a + b)
print(" Nombre d'arbres par genre :")
for genre, nb in par_genre.sortBy(lambda x: x[1], ascending=False).collect():
    print("   ", genre, ":", nb)

sc.stop()