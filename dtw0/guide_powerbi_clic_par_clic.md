# Guide Power BI Clic par Clic - DTW0

## Objectif
Construire 1 page dashboard propre pour analyser les magasins.

## Page a construire
- Page 1: Vue reseau magasins

## Etape 1 - Import des donnees
1. Ouvrir Power BI Desktop.
2. Cliquer Accueil > Obtenir les donnees > Texte/CSV.
3. Choisir le fichier stores_data.csv.
4. Cliquer Transformer les donnees.

## Etape 2 - Nettoyage dans Power Query
1. Supprimer les doublons:
2. Selectionner id magasin.
3. Accueil > Supprimer les lignes > Supprimer les doublons.
4. Nettoyer ouverture le dimanche:
5. Remplacer nan par False.
6. Changer le type en Vrai/Faux.
7. Nettoyer weekly opening hours:
8. Selectionner la colonne.
9. Transformer > Extraire > Texte avant le delimiteur heure.
10. Remplacer les vides par la mediane.
11. Type de donnee en nombre entier.
12. Nettoyer CA:
13. Remplacer le symbole euro par vide.
14. Type de donnee en nombre entier.
15. Nettoyer la date:
16. Changer opening date en Date avec locale francaise si necessaire.
17. Nettoyer ville:
18. Transformer > Format > Nettoyer puis Supprimer les espaces.
19. Harmoniser casse (ex: Marseille, Lyon, Paris).
20. Supprimer numero magasin.
21. Renommer colonnes:
22. id magasin -> id_magasin
23. opening date -> date_ouverture
24. weekly opening hours -> heures_ouverture_hebdomadaires
25. ouverture le dimanche -> ouvert_le_dimanche
26. CA -> chiffre_affaires
27. numero de telephone -> numero_telephone
28. Cliquer Fermer et appliquer.

## Etape 3 - Creer table calendrier
1. Aller sur Modelisation > Nouvelle table.
2. Coller:

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

3. Trier Calendrier[Mois] par Calendrier[MoisNum].
4. Vue Modele > creer relation:
5. Calendrier[Date] -> Magasins[date_ouverture].

## Etape 4 - Creer mesures DAX
1. Modelisation > Nouvelle mesure.
2. Creer:

```DAX
CA Total = SUM(Magasins[chiffre_affaires])
Nb Magasins = DISTINCTCOUNT(Magasins[id_magasin])
CA Moyen Magasin = DIVIDE([CA Total], [Nb Magasins])
Magasins Ouverts Dimanche = CALCULATE(DISTINCTCOUNT(Magasins[id_magasin]), Magasins[ouvert_le_dimanche] = TRUE())
Taux Ouverture Dimanche = DIVIDE([Magasins Ouverts Dimanche], [Nb Magasins])
Heures Moyennes = AVERAGE(Magasins[heures_ouverture_hebdomadaires])
```

## Etape 5 - Construire la page 1
1. Renommer la page: Vue reseau magasins.
2. Inserer 4 cartes en haut:
3. Carte 1: CA Total
4. Carte 2: Nb Magasins
5. Carte 3: CA Moyen Magasin
6. Carte 4: Taux Ouverture Dimanche
7. Inserer histogramme colonnes:
8. Axe: ville
9. Valeur: CA Total
10. Inserer courbe:
11. Axe: Calendrier[AnneeMois]
12. Valeur: CA Total
13. Inserer barres horizontales:
14. Axe: id_magasin
15. Valeur: CA Total
16. Filtre visuel Top N = 10
17. Inserer donut:
18. Legende: ouvert_le_dimanche
19. Valeur: Nb Magasins
20. Inserer tableau detail:
21. Champs: id_magasin, ville, heures_ouverture_hebdomadaires, chiffre_affaires, ouvert_le_dimanche

## Etape 6 - Ajouter filtres
1. Inserer segment Annee avec Calendrier[Annee].
2. Inserer segment Ville avec Magasins[ville].
3. Inserer segment Ouvert dimanche avec Magasins[ouvert_le_dimanche].

## Etape 7 - Verifications finales
1. Filtrer une ville et verifier que tous les visuels changent.
2. Verifier que le nombre de magasins est sans doublons.
3. Verifier que la somme CA correspond a la table.
