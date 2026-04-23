# Guide Power BI Clic par Clic - DTW2

## Objectif
Construire 2 pages dashboard (compactes) pour repondre aux questions du sujet dtw2.

## Pages a construire
- Page 1: Vue globale activite commerciale
- Page 2: Efficacite vendeurs et cout de service

## Etape 1 - Charger les donnees
1. Executer etl_pipeline.py pour generer 05_load_postgres.sql.
2. Charger les tables en PostgreSQL.
3. Ouvrir Power BI Desktop.
4. Accueil > Obtenir les donnees > PostgreSQL.
5. Importer:
6. dim_date
7. dim_seller
8. dim_customer
9. dim_product
10. dim_geo
11. fact_sales_activity

## Etape 2 - Relations du modele
1. Aller en vue Modele.
2. Verifier relations:
3. fact_sales_activity[id_dim_date] -> dim_date[id_dim_date]
4. fact_sales_activity[id_dim_seller] -> dim_seller[id_dim_seller]
5. fact_sales_activity[id_dim_customer] -> dim_customer[id_dim_customer]
6. fact_sales_activity[id_dim_product] -> dim_product[id_dim_product]
7. fact_sales_activity[id_dim_geo] -> dim_geo[id_dim_geo]

## Etape 3 - Mesures DAX
1. Modelisation > Nouvelle mesure.
2. Creer:

```DAX
CA Net = SUM(fact_sales_activity[net_sales_amount])
CA Brut = SUM(fact_sales_activity[gross_sales_amount])
Montant Promesses = SUM(fact_sales_activity[expected_amount])
Km Total = SUM(fact_sales_activity[km_travelled])
Cout Terrain Total =
    SUM(fact_sales_activity[travel_expense]) +
    SUM(fact_sales_activity[road_toll]) +
    SUM(fact_sales_activity[fuel_cost]) +
    SUM(fact_sales_activity[hotel_cost]) +
    SUM(fact_sales_activity[meal_cost]) +
    SUM(fact_sales_activity[misc_cost])
Nb Ventes = CALCULATE(COUNTROWS(fact_sales_activity), fact_sales_activity[net_sales_amount] > 0)
Nb Promesses = CALCULATE(COUNTROWS(fact_sales_activity), fact_sales_activity[promised_qty] > 0)
Taux Transformation % = DIVIDE([Nb Ventes], [Nb Promesses])
CA par Km = DIVIDE([CA Net], [Km Total])
```

## Page 1 - Vue globale activite commerciale

### 1. KPI
1. Renommer la page en Vue globale.
2. Inserer 5 cartes:
3. CA Net
4. Montant Promesses
5. Km Total
6. Cout Terrain Total
7. Taux Transformation %

### 2. Visuels principaux
1. Inserer histogramme colonnes:
2. Axe: dim_seller[full_name]
3. Valeur: CA Net
4. Inserer courbe:
5. Axe: dim_date[annee] puis dim_date[mois]
6. Valeurs: CA Net et Cout Terrain Total
7. Inserer barres horizontales:
8. Axe: dim_geo[region_name]
9. Valeur: Km Total
10. Inserer donut:
11. Legende: fact_sales_activity[activity_status]
12. Valeur: Nb Ventes

### 3. Filtres
1. Slicer Annee avec dim_date[annee].
2. Slicer Region avec dim_geo[region_name].
3. Slicer Vendeur avec dim_seller[full_name].
4. Slicer Categorie avec dim_product[category_name].

## Page 2 - Efficacite vendeurs et cout client

### 1. Efficacite vendeurs
1. Dupliquer page 1.
2. Renommer en Efficacite et cout.
3. Inserer nuage de points:
4. X: Km Total
5. Y: CA Net
6. Taille: Cout Terrain Total
7. Legende: dim_seller[full_name]
8. Ajouter une carte: CA par Km

### 2. Clients couteux a servir
1. Inserer barres horizontales:
2. Axe: dim_customer[customer_name]
3. Valeur: Cout Terrain Total
4. Filtre Top N = 10 par Cout Terrain Total

### 3. Tableau detail
1. Inserer tableau.
2. Colonnes:
3. dim_date[date_complete]
4. dim_seller[full_name]
5. dim_customer[customer_name]
6. dim_product[product_name]
7. fact_sales_activity[net_sales_amount]
8. fact_sales_activity[km_travelled]
9. fact_sales_activity[travel_expense]
10. fact_sales_activity[fuel_cost]

## Verifier que les resultats du sujet sont visibles
1. Chiffre d'affaires net par vendeur et par mois.
2. Kilometres parcourus par region.
3. Vendeurs moins efficaces via CA par Km.
4. Clients les plus couteux a servir.
5. Taux transformation promesses vers ventes.
