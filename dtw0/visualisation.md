Tu as raison: voici un guide pas à pas, ultra concret, pour construire un dashboard Power BI professionnel, sans deviner.

On part de ton fichier nettoyé stores_data.csv et des règles de changement.txt.

**Objectif**
Créer 1 page principale claire avec:
1. KPI en haut
2. Évolution temporelle
3. Comparaison par ville
4. Top magasins
5. Filtres utiles

**Important**
Tu n’as pas besoin d’importer des visualisations custom. Les visuels natifs Power BI suffisent (Carte, Histogramme, Courbe, Donut, Tableau, Segments/Slicers).

---

**Étape 1: Import des données**
1. Ouvre Power BI Desktop.
2. Accueil > Obtenir des données > Texte/CSV.
3. Choisis le fichier stores_data.csv.
4. Clique Transformer les données.
5. Vérifie les types:
1. id_magasin: Nombre entier
2. date_ouverture: Date
3. heures_ouverture_hebdomadaires: Nombre entier
4. ouvert_le_dimanche: Vrai/Faux
5. chiffre_affaires: Nombre entier
6. ville: Texte
7. numero_telephone: Texte
6. Fermer et appliquer.

---

**Étape 2: Table calendrier (obligatoire pour un vrai dashboard pro)**
1. Modélisation > Nouvelle table.
2. Colle cette formule DAX:

```DAX
Calendrier =
ADDCOLUMNS(
    CALENDAR(MIN(Magasins[date_ouverture]), MAX(Magasins[date_ouverture])),
    "Annee", YEAR([Date]),
    "MoisNum", MONTH([Date]),
    "Mois", FORMAT([Date], "[$-fr-FR]mmmm"),
    "AnneeMois", FORMAT([Date], "YYYY-MM")
)
```

3. Dans la table Calendrier:
1. Clique la colonne Mois
2. Outils de colonne > Trier par colonne > MoisNum
4. Vue Modèle:
1. Relie Calendrier[Date] vers Magasins[date_ouverture]
2. Cardinalité: Un à plusieurs
3. Direction du filtre: simple

---

**Étape 3: Mesures DAX à créer**
Modélisation > Nouvelle mesure, puis crée exactement:

```DAX
CA Total = SUM(Magasins[chiffre_affaires])
```

```DAX
Nb Magasins = DISTINCTCOUNT(Magasins[id_magasin])
```

```DAX
CA Moyen Magasin = DIVIDE([CA Total], [Nb Magasins])
```

```DAX
Magasins Ouverts Dimanche =
CALCULATE(
    DISTINCTCOUNT(Magasins[id_magasin]),
    Magasins[ouvert_le_dimanche] = TRUE()
)
```

```DAX
Taux Ouvert Dimanche = DIVIDE([Magasins Ouverts Dimanche], [Nb Magasins])
```

```DAX
Heures Moyennes = AVERAGE(Magasins[heures_ouverture_hebdomadaires])
```

```DAX
CA Cumul YTD = TOTALYTD([CA Total], Calendrier[Date])
```

Formatte ensuite:
1. CA Total et CA Moyen Magasin: Devise ou nombre avec séparateur
2. Taux Ouvert Dimanche: Pourcentage
3. Heures Moyennes: Nombre décimal (1 chiffre)

---

**Étape 4: Construire la page dashboard (exactement quel visuel mettre)**
Renomme la page: Dashboard Réseau Magasins

**Bloc A (haut): 4 Cartes KPI**
1. Visuel Carte
2. Champ: CA Total
3. Duplique 3 fois et remplace par:
1. Nb Magasins
2. CA Moyen Magasin
3. Taux Ouvert Dimanche

Réglages:
1. Titre activé
2. Unités affichage: Auto ou Thousands
3. Décimales: 0 (sauf taux si besoin 1-2)

**Bloc B (milieu gauche): CA par ville**
1. Visuel Histogramme en colonnes groupées
2. Axe X: ville
3. Valeurs Y: CA Total
4. Trier par CA Total décroissant

Réglages:
1. Data labels activés
2. Quadrillage léger
3. Titre: CA Total par Ville

**Bloc C (milieu droite): évolution CA**
1. Visuel Courbe
2. Axe X: Calendrier[AnneeMois]
3. Valeurs Y: CA Total
4. Optionnel: ajoute CA Cumul YTD comme 2e ligne

Réglages:
1. Marqueurs activés
2. Axe X en mode catégoriel
3. Titre: Evolution du Chiffre d’Affaires

**Bloc D (bas gauche): Top magasins**
1. Visuel Barres horizontales groupées
2. Axe Y: id_magasin
3. Valeurs: CA Total
4. Filtres du visuel:
1. Top N
2. Top 10
3. Par valeur: CA Total

Titre: Top 10 Magasins par CA

**Bloc E (bas droite): répartition ouverture dimanche**
1. Visuel Donut
2. Légende: ouvert_le_dimanche
3. Valeurs: Nb Magasins (ou DistinctCount id_magasin)

Titre: Ouverture le Dimanche

**Bloc F (tout en bas): tableau détail**
1. Visuel Tableau
2. Colonnes:
1. id_magasin
2. ville
3. heures_ouverture_hebdomadaires
4. chiffre_affaires
5. ouvert_le_dimanche

---

**Étape 5: Les 3 filtres à mettre (très important)**
Ajoute 3 Segment (Slicer):
1. Calendrier[Annee]
2. ville
3. ouvert_le_dimanche

Place-les en colonne à gauche ou en haut.

Paramètres recommandés:
1. Annee en liste déroulante
2. ville en liste déroulante avec recherche
3. ouvert_le_dimanche en tuiles (True/False)

---

**Étape 6: Rendu professionnel rapide**
1. Affichage > Thèmes > Personnaliser le thème
2. Choisis:
1. Couleur principale: bleu foncé
2. Couleur accent: orange discret
3. Fond page: gris très clair
3. Aligne tout:
1. Sélectionne plusieurs visuels
2. Format > Aligner > Haut/Gauche
3. Format > Distribuer horizontalement/verticalement
4. Garde des espacements réguliers

---

**Étape 7: Vérification avant rendu**
1. Nb Magasins doit être cohérent avec le fichier nettoyé (sans doublons).
2. Quand tu filtres une ville, tous les visuels doivent changer.
3. La courbe temporelle doit suivre la plage de dates attendue.
4. Le donut dimanche doit montrer une répartition réaliste.

---

**Plan ultra simple de construction (ordre conseillé)**
1. Import + types
2. Table Calendrier + relation
3. Mesures DAX
4. 4 cartes KPI
5. Courbe + histogramme ville
6. Top 10 + donut + tableau
7. Slicers + design + contrôle

Si tu veux, je peux te faire la suite en mode atelier:
1. Tu fais seulement les KPI maintenant
2. Tu m’envoies une capture
3. Je te corrige exactement ce qui manque (taille, tri, format, interactions) étape par étape.