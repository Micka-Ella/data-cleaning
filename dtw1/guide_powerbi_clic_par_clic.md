# Guide Power BI Clic par Clic - DTW1

## Objectif
Construire 2 pages dashboard pour le schema etoile de dtw1.

## Pages a construire
- Page 1: Vue executive
- Page 2: Performance commerciale detaillee

## Etape 1 - Import des tables PostgreSQL
1. Ouvrir Power BI Desktop.
2. Cliquer Accueil > Obtenir les donnees > PostgreSQL.
3. Saisir serveur et base DWH.
4. Importer les tables:
5. dim_temps
6. dim_vendeur
7. dim_client
8. dim_produit
9. dim_geo
10. fait_analyse_representant

## Etape 2 - Verifier les relations
1. Aller en vue Modele.
2. Verifier les relations actives:
3. fait_analyse_representant[id_dim_temps] -> dim_temps[id_dim_temps]
4. fait_analyse_representant[id_dim_vendeur] -> dim_vendeur[id_dim_vendeur]
5. fait_analyse_representant[id_dim_client] -> dim_client[id_dim_client]
6. fait_analyse_representant[id_dim_produit] -> dim_produit[id_dim_produit]
7. fait_analyse_representant[id_dim_geo] -> dim_geo[id_dim_geo]

## Etape 3 - Creer mesures DAX
1. Aller sur Modelisation > Nouvelle mesure.
2. Creer:

```DAX
CA Total = SUM(fait_analyse_representant[montant_vente])
Marge Totale = SUM(fait_analyse_representant[marge_estimee])
Rentabilite Nette = SUM(fait_analyse_representant[rentabilite_nette])
Km Total = SUM(fait_analyse_representant[km_parcourus])
Litres Total = SUM(fait_analyse_representant[litres_essence])
Frais Totaux = SUM(fait_analyse_representant[frais_voyage])
Nb Ventes = COUNTROWS(fait_analyse_representant)
CA Moyen Vente = DIVIDE([CA Total], [Nb Ventes])
```

## Page 1 - Vue executive

### 1. Construire les KPI
1. Renommer page en Vue executive.
2. Inserer 5 cartes KPI:
3. CA Total
4. Marge Totale
5. Rentabilite Nette
6. Km Total
7. Frais Totaux

### 2. Ajouter les visuels de synthese
1. Inserer un histogramme colonnes:
2. Axe: dim_vendeur[nom_complet]
3. Valeur: CA Total
4. Inserer une courbe:
5. Axe: dim_temps[annee] puis dim_temps[mois]
6. Valeurs: CA Total et Rentabilite Nette
7. Inserer un donut:
8. Legende: dim_client[segment]
9. Valeur: CA Total

### 3. Ajouter les filtres
1. Slicer Annee avec dim_temps[annee].
2. Slicer Vendeur avec dim_vendeur[nom_complet].
3. Slicer Province avec dim_geo[province].
4. Slicer Categorie avec dim_produit[categorie].

## Page 2 - Performance commerciale detaillee

### 1. Produit et geographie
1. Dupliquer la page 1.
2. Renommer en Performance detaillee.
3. Remplacer un visuel par barres horizontales:
4. Axe: dim_produit[nom]
5. Valeur: CA Total
6. Ajouter matrice geographique:
7. Lignes: dim_vendeur[nom_complet]
8. Colonnes: dim_geo[province]
9. Valeurs: Nb Ventes, Km Total, CA Total

### 2. Analyse rentabilite
1. Inserer nuage de points:
2. X: Km Total
3. Y: CA Total
4. Taille: Rentabilite Nette
5. Legende: dim_vendeur[nom_complet]

### 3. Tableau detail
1. Inserer tableau.
2. Colonnes:
3. dim_temps[date_complete]
4. dim_vendeur[nom_complet]
5. dim_client[segment]
6. dim_produit[nom]
7. fait_analyse_representant[quantite_vendue]
8. fait_analyse_representant[montant_vente]
9. fait_analyse_representant[rentabilite_nette]

## Etape finale - Validation
1. Verifier les 5 questions analytiques du sujet dans les visuels.
2. Tester filtres croises annee/province/vendeur.
3. Verifier coherence des totaux entre cartes et tableaux.
