class Config:
    # ================================================================== #
    # POSITION / FRAIS
    # ================================================================== #
    POSITION_USD = 1000
    FEE_RATE = 0.001

    # ================================================================== #
    # GESTION DU DCA (apports, quantité,...)
    # ================================================================== #
    ALLOW_DCA = False
    FRACTION_INIT = 0.65 #fraction de la position totale à ouvrir initialement (le reste est en attente pour DCA)
    FRACTION_DCA = 0.16  # fraction de la position total à rajouter à chaque DCA (2 au total)

    # ================================================================== #
    # GESTION DES POSITIONS (cycle de vie, clôture simple)
    # ================================================================== #
    TAKE_PROFIT_PNL = 0.02
    STOP_LOSS_PNL = -0.025
    POSITION_DURATION_LIMIT = 3600000 * 24 * 4  # durée max d'une position ouverte (ms)
    MAX_ACTIVE_POSITIONS = 5
    GESTION_MIN_WRITE_INTERVAL_MS = 5000  # throttle des écritures DB "haute fréquence"

    # --- Conditions de clôture avancées ---
    SCORE_ESSOUFFLEMENT_WINDOW = 7            # fenêtre en heures pour le pic glissant du score (score_var)
    SCORE_ESSOUFFLEMENT_DROP = 0.85            # chute de score depuis le pic qui déclenche la clôture
    GAIN_PROGRESS_EPSILON = 0.001             # variation de pnl minimale comptant comme "nouveau plus-haut"
    GAIN_STAGNATION_MS = 3600000 * 24 * 2     # 2 jours sans nouveau plus-haut -> clôture
    HEDGE_MAX_DURATION_MS = 3600000 * 24 * 1  # 1 jour max en hedge -> clôture

    # ================================================================== #
    # HEDGE DYNAMIQUE (seuil / target / ratchet / sorties)
    # ================================================================== #
    OFFSET_LOOSE = 0.01             # offset max du seuil (marché plat, pnl nul) — valeur actuelle conservée
    OFFSET_TIGHT = 0.005             # offset min du seuil (pnl fort et/ou momentum fort aligné)
    PNL_FULL_TIGHTEN = 0.01           # pnl à partir duquel le resserrement lié au gain est maximal
    MOMENTUM_FULL_TIGHTEN = 2       # momentum (en unités d'ATR) à partir duquel le resserrement est maximal
    HEDGE_FATIGUE_STEP = 0.0          # élargissement de l'offset par hedge déjà réalisé sur la position
    HEDGE_MIN_GAIN_MULTIPLE = 3.0     # gain à sécuriser >= X * coût aller-retour du hedge, sinon on ne hedge pas
    RECOVERY_ATR_MULT = 0.5           # distance de la target, en unités d'ATR
    RECOVERY_DEEP_BONUS = 0.005       # marge additionnelle si le hedge a été ouvert en perte notable
    MIN_OFFSET_SPREAD_MULT = 1.5      # l'offset du seuil ne descend jamais sous X * spread courant
    MIN_RECOVERY_SPREAD_MULT = 1.5    # idem pour la target
    HEDGE_TARGET_MAX_GAP_ATR_MULT = 1.5  # écart max entre seuil et target (en unités d'ATR) pour éviter de laisser un hedge ouvert trop longtemps
    TREND_EXHAUSTED_RECOVERY_MULT = 0.5
    HEDGE_COOLDOWN_MS = 300000        # 15 min mini entre une sortie de hedge et la prochaine entrée 
    HEDGE_EXIT_LEVELS = [] # sortie partielle du hedge,palier calculer avec: (exp(3x) - 1)/(exp(3) - 1), mettre [] pour désactiver

    # --- Sorties et hedges partiels ---
    PARTIAL_EXIT_LEVELS = [ # sorties partielles de la position, mettre [] pour désactiver
        (0.015, 0.25),   # +1.5% de pnl -> clôture de 25% de la taille ORIGINALE
        (0.025,  0.25),   # +2.5% de pnl -> 25% supplémentaires clôturés (50% restant, non hedgeable partiellement)
    ]  # liste de (seuil_pnl, fraction_de_la_taille_originale), triée par seuil croissant -- un palier = un index dans nb_partial_exit
    HEDGE_PARTIAL_FRACTION = 1  # fraction de l'exposition RESTANTE couverte à chaque entrée en hedge (1.0 = hedge complet)

    # ================================================================== #
    # SIMULATION DE REMPLISSAGE VIA ORDERBOOK 
    # ================================================================== #
    ORDERBOOK_DEPTH_LIMIT = 50      # nb de niveaux de carnet récupérés par watch_order_book
    FILL_MAX_SLIPPAGE_PCT = 0.003   # écart max toléré entre le prix de référence initial et le VWAP courant avant d'arrêter le remplissage
    FILL_MAX_WAIT_MS = 60000        # délai max pour remplir un ordre avant d'abandonner avec ce qui a été rempli jusque-là
    FILL_MIN_FRACTION = 0.3        # en dessous de cette fraction remplie APRÈS remplissage, on annule l'ordre plutôt que de garder une position/hedge résiduel(le) négligeable
    FILL_MIN_DEPTH_FRACTION = 0.5   # pré-check AVANT engagement : si le carnet (1er snapshot) ne peut absorber au moins cette fraction de la cible dans la tolérance de slippage, l'ordre n'est même pas tenté

    # ================================================================== #
    # BOUCLE PRINCIPALE
    # ================================================================== #
    SLEEP_INTERVAL = 3600      # 1h entre deux rafraîchissements de score
    MANAGER_CALL_SLEEP = 3600  # 1h entre deux appels à W_selection

    # ================================================================== #
    # SÉLECTION / ANALYSE / SCORE
    # ================================================================== #

    # --- Seuils de filtrage (analyse / score) ---
    ADX_MIN = 15
    ADX_MAX = 40
    SLOPE_MIN = 0.2
    VARIATION_MAX = 0.2
    VARIATION_MIN = 0.04
    DELTA_MAX = 0.2
    TREND_FRESH_WINDOW_H = 36         # fenêtre "récente" pour la fraîcheur de tendance (2 jours)
    TREND_BASELINE_WINDOW_H = 120      # fenêtre de référence pour comparer (5 jours)
    TREND_FRESHNESS_MIN_RATIO = 0.65   # part minimale du mouvement total qui doit être récente

    # --- Analyse ---
    ANALYSIS_MIN_CANDLES = 30
    ANALYSIS_SEMAPHORE = 10       # nb d'analyses concurrentes max lors du scan
    ANALYSIS_LIMIT_OHLCV = 150    # bougies 1h récupérées pour analyse()
    ANALYSIS_TAIL_LEN = 40        # bougies utilisées pour la régression
    SPREAD_MAX = 0.005            # spread max accepté (0.5%)
    SCORE_SCAN_MIN = 0.6          # score() minimal au moment du scan pour retenir un candidat
    SCAN_ADVANCED_SEMAPHORE = 10  # concurrence max pour le filtre avancé (ticker + score())

    # --- Score ---
    SCORE_LIMIT_OHLCV = 60             # bougies 1h récupérées pour score()
    SCORE_SAMPLE_LEN = 17
    SCORE_JUMP_WINDOW = 4              # nb de bougies 5m en arrière pour le jump (~20 min)
    SCORE_JUMP_SENSITIVITY = 3.0       # écarts-types 1h pour saturer scoreJump proche de ±1
    SCORE_VOLUME_WINDOW = 5            # nb de bougies 1h récentes pour la moyenne de volume
    SCORE_VOLUME_SENSITIVITY = 0.5     # sensibilité du ratio de volume avant saturation
    SCORE_PNL_TOLERANCE_K = 5.0        # force de la tolérance pnl sur un trend_score négatif
    SCORE_W_REG = 0.85                 # poids régression 1h dans trend_score
    SCORE_W_ADX = 0.05                  # poids force ADX
    SCORE_W_JUMP = 0.05                # poids jump
    SCORE_W_VOLUME = 0.05               # poids confirmation volume
    # somme des SCORE_W_* = 1.0

    # --- Sélection (waitlist / scan) ---
    MIN_QUOTE_VOLUME_24H = 500_000
    FUNDING_RATE_MAX = 0.0007
    MAX_SELECTION = 15                        # taille max du top renvoyé par selection()
    POSITION_DURATION_WAIT_MS = 3600000 * 24  # durée max avant réévaluation forcée

    TRADFI_EXCLUDED_BASES = {
        "TSLA", "NVDA", "AAPL", "GOOGL", "MSFT", "META", "ORCL", "INTC",
        "TSM", "MSTR", "COIN", "CRCL", "HOOD", "MU", "SNDK", "AMZN",
        "QQQ", "EWJ", "EWY",
        "NVDL", "XAU", "XAG", "WTI"
    }

    # ================================================================== #
    # BASE DE DONNÉES
    # ================================================================== #
    DB_PATH = "algo.db"     # chemin UNIQUE, partagé entre database.py et db.py
    DB_TIMEOUT = 5000       # ms, PRAGMA busy_timeout
    LOG_RETENTION_DAYS = 2  # logs conservés X jours avant purge automatique
    ABORTED_POSITION_RETENTION_DAYS = 1  # lignes 'aborted' conservées X jours avant purge automatique

    # ================================================================== #
    # RÉSILIENCE RÉSEAU / WEBSOCKET
    # ================================================================== #
    WS_TICKER_TIMEOUT_S = 90            # délai max sans tick reçu avant de considérer la connexion zombie
    WS_RETRY_BASE_DELAY_S = 1           # délai initial du backoff après une erreur réseau
    WS_RETRY_MAX_DELAY_S = 30           # plafond du backoff exponentiel
    WS_CONSECUTIVE_FAILURES_ALERT = 10  # nb d'échecs consécutifs déclenchant une alerte critique

    # ================================================================== #
    # NOTIFICATIONS (ntfy.sh)
    # ================================================================== #
    NTFY_TOPIC_URL = "https://ntfy.sh/ALGO_TRDHDGE"
    NTFY_TIMEOUT_S = 5