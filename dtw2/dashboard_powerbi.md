# Proposition Dashboard Power BI - DTW2

## Contexte
TP ETL activite commerciale avec sources heterogenes et modele etoile cible:
- dim_date
- dim_seller
- dim_customer
- dim_product
- dim_geo
- fact_sales_activity

Objectif: un dashboard compact mais complet, conforme aux questions d'analyse du sujet.

## Nettoyage des donnees (deja applique par etl_pipeline.py)
- Standardisation des codes metier seller/customer/product avec upper(trim()).
- Harmonisation des dates multi-formats (YYYY-MM-DD, YYYY/MM/DD, DD/MM/YYYY, DD-MM-YYYY).
- Conversion des decimaux avec virgules en numerique.
- Nettoyage des espaces parasites et casses incoherentes.
- Normalisation des statuts (order_status, promise_status).
- Recalcul expected_amount quand absent dans Excel.
- Calcul net_sales_amount = quantity * unit_price * (1 - discount_pct).
- Agregation route + fuel par vendeur et par date.
- Gestion des references inconnues via lignes dimensions de secours.

Controles QA recommandes:
- Verifier part de lignes source_type ORDER vs PROMISE.
- Verifier coherence net_sales_amount <= gross_sales_amount.
- Verifier dates/faits relies a dim_date.

## Modele Power BI
- Importer les 6 tables SQL generees.
- Relations actives depuis fact_sales_activity vers les 5 dimensions.
- Utiliser dim_date pour tous les axes temporels.

## Mesures DAX conseillees
- CA Net = SUM(fact_sales_activity[net_sales_amount])
- CA Brut = SUM(fact_sales_activity[gross_sales_amount])
- Montant Promesses = SUM(fact_sales_activity[expected_amount])
- Km Total = SUM(fact_sales_activity[km_travelled])
- Cout Terrain Total = SUM(fact_sales_activity[travel_expense]) + SUM(fact_sales_activity[road_toll]) + SUM(fact_sales_activity[fuel_cost]) + SUM(fact_sales_activity[hotel_cost]) + SUM(fact_sales_activity[meal_cost]) + SUM(fact_sales_activity[misc_cost])
- Nb Ventes = CALCULATE(COUNTROWS(fact_sales_activity), fact_sales_activity[net_sales_amount] > 0)
- Nb Promesses = CALCULATE(COUNTROWS(fact_sales_activity), fact_sales_activity[promised_qty] > 0)
- Taux Transformation % = DIVIDE([Nb Ventes], [Nb Promesses])
- Efficacite CA par Km = DIVIDE([CA Net], [Km Total])

## Dashboard propose (1 page, style image)
Titre page: Sales Activity Analysis

Visuels:
- Carte KPI 1: CA Net
- Carte KPI 2: Montant Promesses
- Carte KPI 3: Cout Terrain Total
- Carte KPI 4: Taux Transformation %
- Histogramme colonnes: CA Net par Vendeur
- Courbe: CA Net et Cout Terrain par Mois
- Barres horizontales: Kilometres par Region
- Barres horizontales: Clients les plus couteux a servir (Cout Terrain)
- Donut: Repartition des statuts d'activite (order_status/promise_status)
- Tableau detail: vendeur, client, produit, CA Net, Km, Cout Terrain

Filtres (slicers):
- Annee
- Region
- Vendeur
- Categorie produit
- Statut

## Resultats obligatoires du sujet visibles dans ce dashboard
- Chiffre d'affaires net par vendeur et par mois.
- Kilometres parcourus par region.
- Vendeurs les moins efficaces (beaucoup de km, peu de CA) via CA par km.
- Clients les plus couteux a servir.
- Taux de transformation promesses de vente vers ventes.
