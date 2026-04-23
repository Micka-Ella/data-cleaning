# Proposition Dashboard Power BI - DTW1

## Contexte
Datawarehouse en etoile deja alimente avec:
- dim_temps
- dim_vendeur
- dim_client
- dim_produit
- dim_geo
- fait_analyse_representant

Objectif: dashboard de pilotage commercial des representants.

## Nettoyage des donnees (deja applique par ETL, a verifier en QA)
- Normalisation des champs texte (trim, upper/title selon colonnes).
- Conversion des types numeriques et dates.
- Nettoyage des apostrophes pour insertion SQL.
- Mapping geographique des villes vers provinces.
- Rejet des lignes orphelines dans la table de faits (cles manquantes).
- Harmonisation des booleens (est_weekend, actif).

Controles a faire dans Power BI:
- Aucun fait sans lien dimensionnel.
- Aucun montant negatif inattendu (montant_vente, marge_estimee, rentabilite_nette).
- Coherence CA total = somme des lignes de faits.

## Modele Power BI
- Importer les 6 tables du schema etoile.
- Relations actives:
  - fait_analyse_representant[id_dim_temps] -> dim_temps[id_dim_temps]
  - fait_analyse_representant[id_dim_vendeur] -> dim_vendeur[id_dim_vendeur]
  - fait_analyse_representant[id_dim_client] -> dim_client[id_dim_client]
  - fait_analyse_representant[id_dim_produit] -> dim_produit[id_dim_produit]
  - fait_analyse_representant[id_dim_geo] -> dim_geo[id_dim_geo]

## Mesures DAX conseillees
- CA Total = SUM(fait_analyse_representant[montant_vente])
- Quantite Totale = SUM(fait_analyse_representant[quantite_vendue])
- Marge Totale = SUM(fait_analyse_representant[marge_estimee])
- Rentabilite Nette Totale = SUM(fait_analyse_representant[rentabilite_nette])
- Km Total = SUM(fait_analyse_representant[km_parcourus])
- CA Moyen Vente = AVERAGE(fait_analyse_representant[montant_vente])

## Dashboard propose (1 page, style image)
Titre page: Performance Commerciale Representants

Visuels:
- Carte KPI 1: CA Total
- Carte KPI 2: Marge Totale
- Carte KPI 3: Rentabilite Nette Totale
- Carte KPI 4: Km Total
- Histogramme colonnes: CA Total par Vendeur (nom_complet)
- Courbe: Evolution mensuelle CA + Rentabilite (annee/mois)
- Barres horizontales: Top Produits par CA
- Matrice: Vendeur x Province avec Nb Visites et Km
- Donut: CA par Segment client

Filtres (slicers):
- Annee
- Trimestre
- Vendeur
- Province
- Categorie produit

## Resultats a voir absolument (alignes sujet)
- Performance vendeurs: nb ventes, CA total, CA moyen, km.
- Ventes par produit et categorie.
- Evolution mensuelle CA/frais/rentabilite.
- Couverture geographique des vendeurs.
- Rentabilite par segment client.
