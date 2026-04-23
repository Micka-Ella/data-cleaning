# Proposition Dashboard Power BI - DTW0

## Contexte
Jeu de donnees magasins avec informations d'ouverture, horaires, CA et ville.
Objectif: produire un dashboard simple et lisible en une page.

## Nettoyage des donnees (a appliquer ou verifier)
- Supprimer les doublons sur id magasin: 14, 33, 68, 88.
- Normaliser ouverture le dimanche:
  - remplacer nan par False
  - convertir le type en booleen (True/False)
- Nettoyer weekly opening hours:
  - retirer le mot heures
  - convertir en numerique
  - remplacer les valeurs vides par la mediane
- Nettoyer CA:
  - retirer le symbole euro
  - convertir en numerique
- Nettoyer opening date:
  - convertir en type Date (format JJ-MM-AAAA)
- Standardiser ville:
  - trim espaces
  - harmoniser casse (ex: MARSEILLE, lyon -> Marseille, Lyon)
- Supprimer numero magasin (redondant avec id magasin)
- Renommer les colonnes pour Power BI:
  - id magasin -> id_magasin
  - opening date -> date_ouverture
  - weekly opening hours -> heures_ouverture_hebdomadaires
  - ouverture le dimanche -> ouvert_le_dimanche
  - CA -> chiffre_affaires
  - numero de telephone -> numero_telephone

## Modele Power BI
- Table principale: Magasins
- Table calendrier: Calendrier reliee sur date_ouverture
- Relation: Calendrier[Date] (1) -> Magasins[date_ouverture] (*)

## Mesures DAX conseillees
- CA Total = SUM(Magasins[chiffre_affaires])
- Nb Magasins = DISTINCTCOUNT(Magasins[id_magasin])
- CA Moyen par Magasin = DIVIDE([CA Total], [Nb Magasins])
- Nb Magasins Ouverts Dimanche = CALCULATE(DISTINCTCOUNT(Magasins[id_magasin]), Magasins[ouvert_le_dimanche] = TRUE())
- Taux Ouverture Dimanche = DIVIDE([Nb Magasins Ouverts Dimanche], [Nb Magasins])
- Heures Moyennes = AVERAGE(Magasins[heures_ouverture_hebdomadaires])

## Dashboard propose (1 page, style image)
Titre page: Analyse Reseau Magasins

Visuels:
- Carte KPI 1: CA Total
- Carte KPI 2: Nb Magasins
- Carte KPI 3: CA Moyen par Magasin
- Carte KPI 4: Taux Ouverture Dimanche
- Histogramme colonnes: CA Total par Ville
- Courbe: Evolution CA Total par Mois
- Barres horizontales: Top 10 Magasins par CA
- Donut: Repartition Ouvert Dimanche (True vs False)
- Tableau detail: id_magasin, ville, heures_ouverture_hebdomadaires, chiffre_affaires, ouvert_le_dimanche

Filtres (slicers):
- Annee
- Ville
- Ouvert le dimanche

## Lecture metier attendue
- Identifier les villes avec plus fort CA.
- Suivre la tendance du CA dans le temps.
- Comparer performance des magasins.
- Evaluer l'impact de l'ouverture dominicale.
