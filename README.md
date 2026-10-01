# Algo - Bot de trading algorithmique

Bot de trading algorithmique en Python, asynchrone, ciblant les marchés à terme perpétuels mais adaptable à d'autre marchés. Il tourne actuellement en mode dry-run / paper trading : toutes les décisions sont prises et simulées sur des données de marché réelles, sans jamais envoyer d'ordre réel à l'exchange.

> ⚠️ **Avertissement** - Ce projet est un outil d'apprentissage et d'expérimentation personnelle. Il ne constitue en aucun cas un conseil en investissement. Aucune garantie de performance n'est associée à ce code.

---

## Sommaire

- [Genèse du projet](#genèse-du-projet)
- [Philosophie du bot](#philosophie-du-bot)
- [Fonctionnement général](#fonctionnement-général)
- [Stratégie](#stratégie)
- [Gestion des positions](#gestion-des-positions)
- [Base de données](#base-de-données)
- [Résilience, logs et notifications](#résilience-logs-et-notifications)
- [Librairies utilisées](#librairies-utilisées)
- [Licence](#licence)

---

## Genèse du projet

Avant d'arriver à ce bot, de très nombreuses stratégies de trading, inventées ou reprises, ont été implémentées et testées en Python. Ce dépôt ne contient que l'aboutissement de ce travail : les nombreuses autres tentatives, moins abouties ou moins intéressantes à documenter, n'y figurent pas.

Les pistes explorées par le passé couvraient un spectre assez large :

- **Arbitrage** (inter-exchanges, inter-marchés)
- **Analyse de tendance** : régression linéaire et polynomiale, réduction gaussienne, filtrage du bruit sur les séries de prix
- **Analyse on-chain** : repérage de mouvements de portefeuilles susceptibles d'influencer le prix
- **Options** : quelques stratégies exploratoires 

Ces expérimentations ciblaient les cryptomonnaies avant tout parce que c'est un marché très volatil, avec des données et un accès au passage d'ordres facilement accessibles en Python (API ouvertes, pas de barrière à l'entrée comparé aux marchés traditionnels).

Ce projet n'est pas porté par un trader professionnel : il a aussi servi (et sert encore) de support d'apprentissage pour comprendre les bases de la finance de marché : fonctionnement d'un carnet d'ordres, notion de capitalisation, et les outils principaux qu'on y trouve (effet de levier, prêt, options...).

**Constat après backtests et dry-runs de plusieurs mois sur toutes ces stratégies : aucune n'a généré de profit régulier.** Avec le recul, l'explication principale est simple : ces stratégies étaient conçues pour prendre des risques élevés dans l'espoir de générer entre 1 % et 3 % de gain par jour, un objectif évidemment illusoire sur la durée.

## Philosophie du bot

Ce constat a directement orienté la conception de l'algorithme : plutôt que de chercher une "stratégie miracle" à haut rendement, l'accent est mis sur une gestion de position intelligente, mesurée et paramétrable. Le signal d'entrée reste relativement simple (détection de tendance) mais est facilement modifiable ; l'essentiel de la complexité et de l'effort de conception a été investi dans :

- la simulation réaliste des remplissages (contre l'orderbook réel, avec tolérance de slippage),
- un système de couverture dynamique et partiel pour limiter le drawdown sans sur-réagir au bruit,
- des sorties échelonnées plutôt que des clôtures "tout ou rien",
- une résilience opérationnelle (reprise après crash, gestion des erreurs réseau, logs persistés).

L'idée sous-jacente : une gestion de risque robuste a plus de chances de produire un résultat cohérent sur la durée qu'un signal d'entrée sophistiqué mais mal accompagné.

## Fonctionnement général

Le bot tourne en boucle continue :

1. Toutes les heures, il scanne l'ensemble des marchés perpétuels USDT éligibles sur Bybit, calcule un score de détection de tendance pour chacun, et met à jour une waitlist des meilleurs candidats.
2. Pour chaque place libre parmi les 5 positions actives maximum (configurable), il ouvre une position sur le meilleur candidat disponible.
3. Chaque position ouverte est ensuite gérée par une tâche asyncio dédiée, tournant en parallèle des autres, qui suit le marché en websocket et prend les décisions de gestion (apport, hedge, sortie partielle, clôture) en temps réel.
4. Un handler route tous les logs du code vers la base SQLite, en plus de la console.
5. En cas de crash, un mécanisme de reprise relance proprement tout ce qui était en cours au redémarrage.

## Stratégie

### Détection

Pour chaque marché éligible, `analyse()` récupère ~150 bougies 1h et calcule :

- une régression linéaire normalisée : pente de la tendance et dispersion (delta, percentile 95) autour de cette tendance,
- l'ADX (force de la tendance),
- la variation de prix sur la fenêtre récente,
- un ratio de fraîcheur de tendance : compare le mouvement récent au mouvement de référence sur une fenêtre plus longue, pour écarter les tendances déjà bien amorcées ou en cours de retournement,
- un score de détection combinant ces sous-scores.

Des filtres sur delta, ADX, pente, variation et fraîcheur de tendance déterminent l'éligibilité d'un candidat LONG ou SHORT.

### Score de suivi

Une fois un candidat retenu (ou en cours de vie), `score()` quantifie à quel point le marché continue de suivre sa tendance, via plusieurs sous-scores bornés dans [-1, 1] :

- **reg_score** : pente de la régression 1h (poids dominant),
- **adx_score** : force de tendance,
- **jump_score** : mouvement brutal récent (5 min), normalisé par la volatilité horaire,
- **volume_score** : confirmation ou essoufflement par le volume,
- **delta** (dispersion) : pas un score directionnel, mais un facteur de confiance qui atténue l'ensemble si le signal est bruité,
- **pnl** : pas un score en compétition, mais une tolérance asymétrique qui n'amortit qu'un signal négatif, et seulement si la position est déjà gagnante.

Un bonus de durée décroissant (logarithmique) est ajouté pour favoriser les positions jeunes.

### Filtres additionnels avant ouverture

Avant l'ouverture réelle d'une position, un filtre "coûteux" en réseau écarte les candidats dont :

- le spread est trop large,
- le volume 24h est insuffisant,
- le funding rate est défavorable au sens de la position.

## Gestion des positions

Chaque position vit dans sa propre tâche asyncio, qui suit le carnet d'ordres en websocket et gère :

- **Simulation de remplissage réaliste** : tous les ordres (ouverture, apport, sortie, hedge) sont simulés contre l'orderbook réel, avec tolérance de slippage, pré-check de profondeur avant engagement, et persistance anti-crash.
- **DCA / apports** (optionnel) : jusqu'à deux apports si le prix s'éloigne du prix d'entrée au-delà de seuils basés sur l'ATR.
- **Sorties partielles** : clôture progressive d'une fraction de la position à des paliers de PnL définis, pour sécuriser du gain sans sortir intégralement.
- **Hedge dynamique et partiel** : un seuil de déclenchement se resserre en fonction du PnL et du momentum multi-échelle (2 min / 10 min / 30 min / 1h), avec un offset minimal borné par le spread courant. Le hedge ne couvre qu'une fraction paramétrable de l'exposition, avec une target de sortie calculée pour garantir un breakeven, puis des sorties par paliers à mesure que le prix récupère.
- **Réévaluation horaire du score et de l'ATR** : avec détection d'essoufflement de tendance (chute du score depuis son pic sur une fenêtre glissante) qui force un resserrement maximal du hedge et peut déclencher une clôture.
- **Conditions de clôture automatique** : durée maximale de position, hedge resté ouvert trop longtemps, stagnation du gain (pas de nouveau plus-haut de PnL depuis X jours), essoufflement de tendance.
- **Écritures DB throttlées** pour les mises à jour haute fréquence (bid/ask/pnl), mais flush immédiat sur tout événement structurant (apport, hedge, clôture, score).


## Base de données

SQLite en mode WAL. Tables principales :

- `positions` - cycle de vie complet d'une position (entrée A/B/C, seuils d'apport, hedge, PnL, drawdown, frais...)
- `hedges` - historique des couvertures liées à une position
- `pending_orders` - suivi des remplissages en cours (anti-crash, permet de détecter un remplissage interrompu au redémarrage)
- `waitlist` - candidats retenus au dernier cycle de sélection
- `logs` - tous les logs applicatifs, avec purge automatique après X jours

## Résilience, logs et notifications

- **Reprise après crash** : au démarrage, le bot nettoie les ordres restés bloqués en filling, retente les positions restées en pending, puis relance une tâche de gestion pour chaque position déjà active/hedged.
- **Gestion différenciée des erreurs réseau** : erreurs fatales (authentification, permissions, symbole delisté) → arrêt de la tâche et alerte critique ; erreurs réseau transitoires → backoff exponentiel et reconnexion.
- **Détection de websocket zombie** via timeout avec alerte après un nombre configurable d'échecs consécutifs.
- **Logs persistés** : tous les logs existants sont automatiquement routés vers la table logs via un handler custom (DBLogHandler), sans rien changer aux appels déjà présents dans le code.
- **Notifications push** :  (Android, via [ntfy.sh](https://ntfy.sh)) sur les alertes critiques.

## Librairies utilisées

| Domaine | Librairie |
|---|---|
| Exchange (données + ordres simulés) | `ccxt.pro` (websocket, Bybit) |
| Asynchrone | `asyncio` |
| Base de données | `aiosqlite` (SQLite, mode WAL) |
| Analyse de données | `pandas`, `numpy` |
| Indicateurs techniques | `pandas_ta_classic` (ADX, ATR) |
| Requêtes HTTP (notifications) | `aiohttp` |
| Données actions (module annexe) | `yfinance` |

## Licence

Projet personnel à but d'apprentissage — à adapter selon l'usage souhaité (ex. MIT).
