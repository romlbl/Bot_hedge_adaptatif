import asyncio
import time
import logging
import ccxt
import aiohttp
from collections import deque

import pandas as pd
import pandas_ta_classic as ta  # noqa: F401  (nécessaire pour l'accessor df.ta)

from config import Config
from db import Database
from strategy import score as compute_score

def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


class PositionManager:
    def __init__(self, exchange, db: Database):
        self.exchange = exchange
        self.db = db
        self.tasks: set[asyncio.Task] = set()

    async def recover_active_positions(self) -> None:
        active_positions = await self.db.get_active_positions()
        for id_pos, symbol, side in active_positions:
            logging.info(f"Reprise de la position {id_pos} ({symbol}, {side}).")
            self.spawn_gestion(id_pos)

    async def recover_stale_pending_orders(self) -> None:
        """
        Au démarrage : les lignes 'filling' de pending_orders correspondent à des
        remplissages interrompus par un crash -- aucune tâche vivante ne les
        reprendra jamais (_execute_fill_order n'est rappelée avec un id_order
        existant nulle part). On les marque 'aborted' pour l'hygiène de la table.

        Note : le/la parent(e) (positions/hedges) n'a reçu AUCUNE mise à jour tant
        que _execute_fill_order n'était pas retournée à son appelant -- il n'y a
        donc rien d'autre à réconcilier ici que le log d'alerte.
        """
        stale = await self.db.get_open_pending_orders()
        for order in stale:
            logging.warning(
                f"recover_stale_pending_orders: ordre {order['id']} ({order['kind']}, "
                f"position {order['id_position']}) resté 'filling' après un redémarrage — "
                f"{order['filled_size']:.6f}/{order['target_size']:.6f} rempli avant le crash, "
                f"marqué 'aborted'."
            )
            await self.db.update_pending_order(
                order["id"], status="aborted", updated_at=int(time.time() * 1000)
            )

    async def recover_pending_positions(self) -> None:
        """
        Au démarrage : retente l'ouverture des positions restées 'pending' après
        un crash (jamais passées à 'active'). Réutilise _finalize_open sur la
        ligne déjà existante -- aucune réinsertion, id_pos est déjà connu.
        """
        rows = await self.db.execute_read(
            "SELECT id, symbol, side FROM positions WHERE status = 'pending'"
        )
        for id_pos, symbol, side in rows:
            logging.warning(
                f"recover_pending_positions: position {id_pos} ({symbol}, {side}) restée "
                f"'pending' après un redémarrage — nouvelle tentative d'ouverture."
            )
            succes = await self._finalize_open(id_pos, symbol, side)
            if succes:
                self.spawn_gestion(id_pos)
            # _finalize_open a déjà marqué la ligne 'aborted' en cas d'échec --
            # rien d'autre à faire ici.

    def spawn_gestion(self, id_pos: int) -> asyncio.Task:
        task = asyncio.create_task(self.W_gestionPos(id_pos))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return task

    async def _alert_critical(self, message: str) -> None:
        """Point d'accroche unique pour toute alerte critique : log + notification
        push Android via ntfy.sh. Ne doit JAMAIS lever d'exception -- un échec
        d'envoi de notification ne doit jamais interrompre la gestion d'une position."""
        logging.critical(message)
        try:
            async with aiohttp.ClientSession() as session:
                await session.post(
                    Config.NTFY_TOPIC_URL,
                    data=message.encode("utf-8"),
                    headers={
                        "Title": "Trading bot - alerte critique",
                        "Priority": "urgent",
                        "Tags": "rotating_light",
                    },
                    timeout=aiohttp.ClientTimeout(total=Config.NTFY_TIMEOUT_S),
                )
        except Exception as e:
            logging.error(f"_alert_critical: échec envoi notification ntfy: {e}")

    async def _handle_ws_failure(self, id_pos: int, symbol: str, consecutive_failures: int) -> None:
        """Centralise l'alerting sur échecs répétés de watch_ticker (réseau ou zombie)."""
        if consecutive_failures == Config.WS_CONSECUTIVE_FAILURES_ALERT:
            await self._alert_critical(
                f"Position {id_pos} ({symbol}) : {consecutive_failures} échecs consécutifs de "
                f"watch_ticker. La position n'est plus surveillée en temps réel — vérifier "
                f"la connectivité à l'exchange."
            )

    @staticmethod
    def _atr_from_ohlcv(ohlcv) -> float:
        """ATR 1h (14 périodes) à partir d'un OHLCV déjà récupéré — pas d'appel réseau."""
        df = pd.DataFrame(ohlcv, columns=["time", "open", "high", "low", "close", "volume"])
        df[["open", "high", "low", "close", "volume"]] = df[
            ["open", "high", "low", "close", "volume"]
        ].astype(float)
        df.ta.atr(length=14, append=True)
        return float(df["ATRr_14"].iloc[-1])

    async def _fetch_atr(self, symbol: str) -> float:
        """Utilisé quand aucun ATR n'est encore en mémoire (reprise après redémarrage,
        ou refresh horaire dans W_gestionPos) : fait l'appel réseau lui-même."""
        ohlcv = await self.exchange.fetch_ohlcv(symbol, "1h", limit=Config.ANALYSIS_LIMIT_OHLCV)
        if not ohlcv:
            return 0.0
        return self._atr_from_ohlcv(ohlcv)

    @staticmethod
    def _simulate_fill_against_book(levels, remaining_size, ref_price, side, slippage_pct):
        """
        Marche les niveaux du carnet (déjà triés du meilleur au moins bon prix,
        format ccxt [[prix, quantité], ...]) et accumule autant que possible dans
        la limite de tolérance de slippage par rapport à ref_price.

        side: 'buy'  -> consomme les asks, accepte prix <= ref_price * (1 + slippage_pct)
              'sell' -> consomme les bids, accepte prix >= ref_price * (1 - slippage_pct)

        Retourne (filled_qty, vwap_price_de_cette_tranche, slippage_hit).
        slippage_hit=True signifie qu'on s'est arrêté À CAUSE de la tolérance
        (le carnet avait peut-être plus de profondeur, mais trop loin du prix
        de référence) -- distinct d'un simple carnet épuisé.
        """
        if remaining_size <= 0 or not levels:
            return 0.0, None, False

        if side == "buy":
            price_limit = ref_price * (1 + slippage_pct)
        else:
            price_limit = ref_price * (1 - slippage_pct)

        filled = 0.0
        cost = 0.0
        slippage_hit = False

        for price, qty in levels:
            if side == "buy" and price > price_limit:
                slippage_hit = True
                break
            if side == "sell" and price < price_limit:
                slippage_hit = True
                break

            take = min(qty, remaining_size - filled)
            if take <= 0:
                break
            filled += take
            cost += take * price
            if filled >= remaining_size:
                break

        vwap = (cost / filled) if filled > 0 else None
        return filled, vwap, slippage_hit

    @staticmethod
    def _theoretical_exit_vwap(levels, size, fallback_price):
        """
        Marche le carnet pour estimer le prix moyen pondéré qu'on obtiendrait en
        vendant/achetant `size` unités MAINTENANT, contre le carnet réel -- pure
        valorisation (aucune tolérance de slippage : on veut le prix vrai, même
        dégradé, pas un prix filtré comme dans _simulate_fill_against_book).

        Si la profondeur récupérée (Config.ORDERBOOK_DEPTH_LIMIT niveaux) ne
        couvre pas toute la taille, le reliquat est valorisé au DERNIER prix
        atteint -- approximation conservatrice qui n'extrapole pas au-delà du
        carnet visible. Pour une position dont la taille dépasse largement ce
        que ces niveaux peuvent absorber, cette approximation SOUS-ESTIME le
        slippage réel au-delà du carnet visible (à garder en tête si tu montes
        vers des tailles de position type 10 000 USDT).
        """
        if size <= 0 or not levels:
            return fallback_price

        filled = 0.0
        cost = 0.0
        last_price = levels[0][0]

        for price, qty in levels:
            take = min(qty, size - filled)
            if take <= 0:
                break
            filled += take
            cost += take * price
            last_price = price
            if filled >= size:
                break

        if filled < size:
            cost += (size - filled) * last_price
            filled = size

        return cost / filled if filled > 0 else fallback_price

    def _round_size(self, symbol: str, raw_size: float) -> float:
        """
        Arrondit raw_size vers le bas au pas de quantité réel du marché (qtyStep /
        lot size, récupéré via load_markets() -- pas un nombre de décimales
        arbitraire). ccxt tronque déjà par défaut dans amount_to_precision (jamais
        de ROUND au-dessus) : on ne dépasse donc jamais le budget visé.
        Retourne 0.0 si le résultat tombe sous le minimum de taille autorisé par
        l'exchange (ordre inexécutable en pratique sur un vrai marché).
        """
        try:
            rounded = float(self.exchange.amount_to_precision(symbol, raw_size))
        except Exception as e:
            logging.error(f"_round_size: échec précision sur {symbol}: {e}")
            return raw_size

        market = self.exchange.market(symbol)
        min_amount = (market.get('limits', {}).get('amount') or {}).get('min') or 0.0
        return rounded if rounded >= min_amount else 0.0

    async def _execute_fill_order(self, symbol: str, side: str, target_size: float, ref_price: float,
                                   id_position: int, kind: str, id_hedge: int | None = None,
                                   check_exit_liquidity: bool = False):
        """
        Exécute un ordre (buy/sell) jusqu'à target_size sur le carnet réel avec sauvegarde anti-crash continue dans pending_orders.
        Un pré-check de profondeur gratuit réutilise la première lecture du carnet pour tout abandonner sans écriture DB si la liquidité
        est inférieure à FILL_MIN_DEPTH_FRACTION. La fonction renvoie (filled_size, vwap_price, status) avec le statut done (remplissage complet),
        partial (interrompu par slippage/timeout mais rempli au moins à FILL_MIN_FRACTION) ou aborted (remplissage négligeable), laissant la
        gestion de l'ordre incomplet à l'appelant.
        """
        if target_size <= 0:
            return 0.0, None, "aborted"

        try:
            first_ob = await asyncio.wait_for(
                self.exchange.watch_order_book(symbol, limit=Config.ORDERBOOK_DEPTH_LIMIT),
                timeout=Config.WS_TICKER_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            logging.warning(
                f"_execute_fill_order: carnet initial indisponible sur {symbol} "
                f"({kind}, id_pos={id_position}), ordre non engagé."
            )
            return 0.0, None, "aborted"

        first_levels = first_ob["asks"] if side == "buy" else first_ob["bids"]
        depth_filled, depth_vwap, _ = self._simulate_fill_against_book(
            first_levels, target_size, ref_price, side, Config.FILL_MAX_SLIPPAGE_PCT
        )
        depth_fraction = depth_filled / target_size if target_size else 0.0

        if depth_fraction < Config.FILL_MIN_DEPTH_FRACTION:
            logging.info(
                f"_execute_fill_order: carnet trop fin sur {symbol} ({kind}, id_pos={id_position}) "
                f"— seulement {depth_fraction:.1%} de la cible ({target_size:.6f}) remplissable "
                f"dans la tolérance de slippage sur le carnet actuel (seuil={Config.FILL_MIN_DEPTH_FRACTION:.0%}), "
                f"ordre non engagé."
            )
            return 0.0, None, "aborted"

        if check_exit_liquidity:
            exit_side = "sell" if side == "buy" else "buy"
            exit_levels = first_ob["bids"] if side == "buy" else first_ob["asks"]
            if not exit_levels:
                logging.info(
                    f"_execute_fill_order: carnet de sortie vide sur {symbol} ({kind}, id_pos={id_position}) "
                    f"— aucune liquidité pour refermer la position visée, ordre non engagé."
                )
                return 0.0, None, "aborted"

            exit_ref_price = exit_levels[0][0]
            exit_filled, _, _ = self._simulate_fill_against_book(
                exit_levels, target_size, exit_ref_price, exit_side, Config.FILL_MAX_SLIPPAGE_PCT
            )
            exit_fraction = exit_filled / target_size if target_size else 0.0

            if exit_fraction < Config.FILL_MIN_DEPTH_FRACTION:
                logging.info(
                    f"_execute_fill_order: carnet de SORTIE trop fin sur {symbol} ({kind}, id_pos={id_position}) "
                    f"— seulement {exit_fraction:.1%} de la cible ({target_size:.6f}) serait remplissable "
                    f"pour refermer la position dans la tolérance de slippage (seuil={Config.FILL_MIN_DEPTH_FRACTION:.0%}), "
                    f"ouverture annulée par précaution."
                )
                return 0.0, None, "aborted"

        now_ms = int(time.time() * 1000)
        id_order = await self.db.insert_pending_order(
            id_position=id_position, id_hedge=id_hedge, kind=kind, side=side,
            target_size=target_size, filled_size=0.0, vwap_price=None,
            ref_price=ref_price, status="filling", started_at=now_ms, updated_at=now_ms,
        )

        filled = 0.0
        cost = 0.0
        started_at = now_ms

        # La première tranche est déjà connue (snapshot du pré-check ci-dessus) --
        # on l'applique directement plutôt que de la jeter et de refaire un appel.
        if depth_filled > 0:
            cost += depth_filled * depth_vwap
            filled += depth_filled
            await self.db.update_pending_order(
                id_order, filled_size=filled, vwap_price=(cost / filled),
                updated_at=int(time.time() * 1000),
            )

        try:
            while filled < target_size:
                if int(time.time() * 1000) - started_at >= Config.FILL_MAX_WAIT_MS:
                    logging.warning(
                        f"_execute_fill_order: timeout sur {symbol} ({kind}, id_pos={id_position}) "
                        f"— rempli {filled:.6f}/{target_size:.6f}."
                    )
                    break

                try:
                    ob = await asyncio.wait_for(
                        self.exchange.watch_order_book(symbol, limit=Config.ORDERBOOK_DEPTH_LIMIT),
                        timeout=Config.WS_TICKER_TIMEOUT_S,
                    )
                except asyncio.TimeoutError:
                    logging.warning(f"_execute_fill_order: pas de mise à jour orderbook sur {symbol}, nouvelle tentative.")
                    continue

                levels = ob["asks"] if side == "buy" else ob["bids"]
                remaining = target_size - filled
                step_filled, step_vwap, slippage_hit = self._simulate_fill_against_book(
                    levels, remaining, ref_price, side, Config.FILL_MAX_SLIPPAGE_PCT
                )

                if step_filled > 0:
                    cost += step_filled * step_vwap
                    filled += step_filled
                    await self.db.update_pending_order(
                        id_order, filled_size=filled, vwap_price=(cost / filled),
                        updated_at=int(time.time() * 1000),
                    )

                if filled >= target_size:
                    break

                if slippage_hit:
                    logging.info(
                        f"_execute_fill_order: slippage max atteint sur {symbol} ({kind}, id_pos={id_position}) "
                        f"— rempli {filled:.6f}/{target_size:.6f}, arrêt du remplissage."
                    )
                    break

            vwap_final = (cost / filled) if filled > 0 else None
            if filled >= target_size:
                status = "done"
            elif filled >= target_size * Config.FILL_MIN_FRACTION:
                status = "partial"
            else:
                status = "aborted"

            await self.db.update_pending_order(
                id_order, status=status, filled_size=filled, vwap_price=vwap_final,
                updated_at=int(time.time() * 1000),
            )
            return filled, vwap_final, status

        except Exception as e:
            logging.error(f"_execute_fill_order: erreur sur {symbol} ({kind}, id_pos={id_position}): {e}")
            await self.db.update_pending_order(id_order, status="aborted", updated_at=int(time.time() * 1000))
            return filled, (cost / filled if filled > 0 else None), "aborted"

    # ------------------------------------------------------------------ #
    # W_openPos
    # ------------------------------------------------------------------ #

    async def W_openPos(self, symbol: str, side: str, score: float):
        """
        Crée la ligne 'positions' puis tente immédiatement son ouverture réelle
        (remplissage simulé Option B contre le carnet). Contrat : retourne
        (id_pos, succes: bool). En cas d'échec, la ligne est marquée 'aborted'
        immédiatement par _finalize_open -- elle ne reste plus jamais bloquée
        en 'pending' en dehors d'un crash (seul recover_pending_positions()
        gère ce cas résiduel).
        """
        id_pos = await self.db.insert_pending_position(
            symbol=symbol, side=side, status="pending", score=score,
        )
        succes = await self._finalize_open(id_pos, symbol, side)
        return id_pos, succes

    async def _finalize_open(self, id_pos: int, symbol: str, side: str) -> bool:
        """
        Logique d'ouverture réelle, partagée entre W_openPos (nouvelle position)
        et recover_pending_positions (reprise sur une ligne 'pending' déjà
        existante après crash). Ne crée JAMAIS de ligne 'positions' -- id_pos
        doit déjà exister. Marque systématiquement la ligne 'aborted' si
        l'ouverture échoue, à quelque étape que ce soit.
        """
        async def _abort(msg: str) -> bool:
            logging.info(msg)
            await self.db.close_position(id_pos, status="aborted", closeTime=int(time.time() * 1000))
            return False

        pos = await self.db.get_position(id_pos)
        if pos is None:
            logging.error(f"W_openPos: position {id_pos} introuvable en DB.")
            return False

        is_long = side == "LONG"
        try:
            # 1) ticker initial (sert de prix de référence pour le fill) + ATR
            ticker, ohlcv = await asyncio.gather(
                self.exchange.fetch_ticker(symbol),
                self.exchange.fetch_ohlcv(symbol, "1h", limit=Config.ANALYSIS_LIMIT_OHLCV),
            )
            ask = ticker.get("ask")
            bid = ticker.get("bid")
            if ask is None or bid is None:
                return await _abort(f"W_openPos: ask/bid indisponible pour {symbol} (position {id_pos}).")

            if not ohlcv:
                return await _abort(f"W_openPos: OHLCV indisponible pour {symbol} (position {id_pos}).")

            atr = self._atr_from_ohlcv(ohlcv)

            # 2) prix de référence initial + taille visée (budget USD / prix de référence),
            # arrondie vers le bas au pas de quantité réel du marché
            ref_price = ask if is_long else bid
            order_side = "buy" if is_long else "sell"
            target_size_raw = (
                Config.POSITION_USD * Config.FRACTION_INIT / ref_price
                if Config.ALLOW_DCA else Config.POSITION_USD / ref_price
            )
            target_size = self._round_size(symbol, target_size_raw)
            if target_size <= 0:
                return await _abort(
                    f"W_openPos: taille visée sur {symbol} (position {id_pos}) sous le "
                    f"minimum de marché après arrondi ({target_size_raw:.8f} -> 0), ouverture annulée."
                )

            # 3) remplissage simulé contre le carnet réel (Option B)
            filled_size, vwap_price, fill_status = await self._execute_fill_order(
                symbol, order_side, target_size, ref_price, id_pos, kind="open_A",
                check_exit_liquidity=True,
            )

            if fill_status == "aborted":
                return await _abort(
                    f"W_openPos: remplissage abandonné sur {symbol} (position {id_pos}) "
                    f"— trop peu de liquidité disponible dans la tolérance de slippage."
                )

            if fill_status == "partial":
                logging.info(
                    f"W_openPos: remplissage partiel sur {symbol} (position {id_pos}) "
                    f"— {filled_size:.6f}/{target_size:.6f} rempli, position ouverte avec ce qui a été rempli."
                )

            size = filled_size

            # 4) photo de marché opposée (référence SL/TP), reprise APRÈS le remplissage
            ticker_after = await self.exchange.fetch_ticker(symbol)
            ask_now = ticker_after.get("ask", ask)
            bid_now = ticker_after.get("bid", bid)

            # 5) seuils d'apport, calculés sur le VWAP RÉELLEMENT obtenu
            seuil_apport_low = vwap_price - 1.1 * atr
            seuil_apport_lower = vwap_price - 2.2 * atr
            seuil_apport_high = vwap_price + 1.1 * atr
            seuil_apport_higher = vwap_price + 2.2 * atr

            # 6) frais, proportionnels à ce qui a RÉELLEMENT été rempli
            fee = size * vwap_price * Config.FEE_RATE
            total_fees = (pos.get("total_fees") or 0.0) + fee
            total_fees_paid = (pos.get("total_fees_paid") or 0.0) + fee
            open_time = int(time.time() * 1000)

            # 7) SL/TP : référence = prix opposé courant, principe inchangé
            sell_price_SL_TP = bid_now if is_long else ask_now
            stop_loss = sell_price_SL_TP * (1 + Config.STOP_LOSS_PNL) if is_long else sell_price_SL_TP * (1 - Config.STOP_LOSS_PNL)
            stop_loss_atr = vwap_price - 1.5 * atr if is_long else vwap_price + 1.5 * atr
            stop_loss = max(stop_loss, stop_loss_atr) if is_long else min(stop_loss, stop_loss_atr)
            take_profit = sell_price_SL_TP * (1 + Config.TAKE_PROFIT_PNL) if is_long else sell_price_SL_TP * (1 - Config.TAKE_PROFIT_PNL)

            # 8) ask_A/bid_A : le côté RÉELLEMENT tradé reçoit le VWAP, l'autre garde
            #    une simple photo de marché (jamais tradé, sert uniquement de référence)
            ask_A = vwap_price if is_long else ask_now
            bid_A = bid_now if is_long else vwap_price

            await self.db.update_position(
                id_pos,
                status="active",
                ask_A=ask_A,
                bid_A=bid_A,
                size_A=size,
                openTime_A=open_time,
                total_fees=total_fees,
                total_fees_paid=total_fees_paid,
                seuil_apport_low=seuil_apport_low,
                seuil_apport_lower=seuil_apport_lower,
                seuil_apport_high=seuil_apport_high,
                seuil_apport_higher=seuil_apport_higher,
                stop_loss=stop_loss,
                take_profit=take_profit,
                atr=atr,
            )

            logging.info(
                f"Position {id_pos} ouverte : {symbol} {side} @ VWAP={vwap_price:.6f} "
                f"(size={size:.6f}, cible={target_size:.6f}, statut fill={fill_status})."
            )
            return True

        except Exception as e:
            logging.error(f"Erreur W_openPos sur {id_pos}: {e}")
            try:
                await self.db.close_position(id_pos, status="aborted", closeTime=int(time.time() * 1000))
            except Exception:
                pass
            return False

    # ------------------------------------------------------------------ #
    # W_gestionPos
    # ------------------------------------------------------------------ #

    async def W_gestionPos(self, id_pos: int):
        """
        Gestion en continu d'une position : websocket sur le symbole, apports,
        seuil/target de hedge, gain/pnl/top_pnl/drawdown, score horaire.

        État gardé en mémoire après la lecture initiale (seule cette tâche écrit
        sur cette ligne). Les écritures DB sont regroupées et throttlées via
        Config.GESTION_MIN_WRITE_INTERVAL_MS, SAUF pour les événements structurants
        (apport, hedge, changement de statut, score) qui sont toujours flushés
        immédiatement pour ne rien perdre en cas de crash.
        """
        pos = await self.db.get_position(id_pos)
        if pos is None:
            logging.error(f"W_gestionPos: position {id_pos} introuvable en DB.")
            return

        symbol = pos["symbol"]
        side = pos["side"]
        is_long = side == "LONG"

        status = pos["status"]
        seuil = pos["seuil"]
        target = pos["target"]
        id_hedge = pos["id_hedge"]
        nb_apport = pos["nb_apport"] or 0
        nb_hedge = pos["nb_hedge"] or 0
        total_fees = pos["total_fees"] or 0.0
        total_fees_paid = pos["total_fees_paid"] or 0.0
        remaining_fraction = pos["remaining_fraction"] if pos["remaining_fraction"] is not None else 1.0
        realized_partial_gain = pos["realized_partial_gain"] or 0.0
        nb_partial_exit = pos["nb_partial_exit"] or 0
        ask_A, bid_A, size_A = pos["ask_A"] or 0.0, pos["bid_A"] or 0.0, pos["size_A"] or 0.0
        ask_B, bid_B, size_B = pos["ask_B"], pos["bid_B"], pos["size_B"]
        ask_C, bid_C, size_C = pos["ask_C"], pos["bid_C"], pos["size_C"]
        seuil_apport_low = pos["seuil_apport_low"]
        seuil_apport_lower = pos["seuil_apport_lower"]
        seuil_apport_high = pos["seuil_apport_high"]
        seuil_apport_higher = pos["seuil_apport_higher"]
        top_pnl = pos["top_pnl"]
        drawdown = pos["drawdown"]
        current_pnl = pos["pnl"] or 0.0
        realized_hedge_gain = (await self.db.execute_read(
                    "SELECT COALESCE(SUM(gain), 0) FROM hedges WHERE id_parent = ? AND status = 'closed'",
                    (id_pos,),))[0][0]
        hedge_side = hedge_size = hedge_entry_price = hedge_open_time = None
        hedge_fees = 0.0
        hedge_original_size = None
        hedge_nb_partial_exit = 0
        hedge_realized_partial_gain = 0.0
        if status == "hedged" and id_hedge:
            hedge = await self.db.get_hedge(id_hedge)
            if hedge:
                hedge_side = hedge["side"]
                hedge_size = hedge["size"] or 0.0
                hedge_entry_price = hedge["ask"] if hedge_side == "LONG" else hedge["bid"]
                hedge_fees = hedge["total_fees"] or 0.0
                hedge_open_time = hedge["openTime"]
                hedge_original_size = hedge["original_size"] or hedge_size
                hedge_nb_partial_exit = hedge["nb_partial_exit"] or 0
                hedge_realized_partial_gain = hedge["realized_partial_gain"] or 0.0
            else:
                logging.error(f"W_gestionPos: hedge {id_hedge} introuvable pour position {id_pos}, retour à 'active'.")
                status = "active"

        hist2M: deque[tuple[int, float]] = deque()
        hist10: deque[tuple[int, float]] = deque()
        hist30: deque[tuple[int, float]] = deque()
        hist1h: deque[tuple[int, float]] = deque()
        sum2 = sum10 = sum30 = sum1h = 0.0

        # ATR en mémoire pour normaliser les offsets de seuil/target — lu depuis la
        # position si déjà connu, sinon récupéré une fois au démarrage de la gestion.
        atr = pos["atr"]
        if not atr:
            atr = await self._fetch_atr(symbol)
        last_score_update = pos["openTime_A"] or int(time.time() * 1000)
        last_write = 0
        pending: dict = {}

        # historique des scores horaires (condition de clôture n°0 : essoufflement)
        score_window_ms = Config.SCORE_ESSOUFFLEMENT_WINDOW * 3600000
        score_history: deque[tuple[int, float]] = deque()
        if pos["score"] is not None:
            seed_score = pos["score_var"] if pos["score_var"] is not None else pos["score"]
            score_history.append((last_score_update, seed_score))

        # dernier "nouveau plus-haut" de pnl (condition de clôture n°4 : stagnation)
        last_progress_time = pos["openTime_A"] or int(time.time() * 1000)

        # flag persistant : une fois l'essoufflement détecté, il reste vrai jusqu'à
        # la clôture (évite de dépendre d'un timing exact entre détection et clôture)
        trend_exhausted = False
        last_hedge_exit_time = None

        logging.info(f"Gestion de la position {id_pos} ({symbol}, {side}) démarrée.")
        consecutive_failures = 0
        while True:
            try:
                try:
                    ob = await asyncio.wait_for(
                        self.exchange.watch_order_book(symbol, limit=Config.ORDERBOOK_DEPTH_LIMIT),
                        timeout=Config.WS_TICKER_TIMEOUT_S,
                    )
                except asyncio.TimeoutError:
                    consecutive_failures += 1
                    logging.warning(
                        f"W_gestionPos: aucune mise à jour d'orderbook reçue depuis {Config.WS_TICKER_TIMEOUT_S}s "
                        f"sur {symbol} (position {id_pos}) — websocket probablement zombie, reconnexion forcée."
                    )
                    await self._handle_ws_failure(id_pos, symbol, consecutive_failures)
                    continue

                consecutive_failures = 0
                asks, bids = ob.get("asks") or [], ob.get("bids") or []
                if not asks or not bids:
                    continue
                ask, bid = asks[0][0], bids[0][0]

                ref_price = bid if is_long else ask
                spread = ask - bid
                spread_perc = spread / ref_price if ref_price else 0.0

                now_ms = int(time.time() * 1000)

                # Ajout du nouveau point dans les 4 fenêtres, somme glissante O(1)
                hist2M.append((now_ms, ref_price)); sum2 += ref_price
                hist10.append((now_ms, ref_price)); sum10 += ref_price
                hist30.append((now_ms, ref_price)); sum30 += ref_price
                hist1h.append((now_ms, ref_price)); sum1h += ref_price

                # Éviction des points expirés, chaque fenêtre gère son propre cutoff
                cutoff2 = now_ms - 120_000
                while hist2M and hist2M[0][0] < cutoff2:
                    _, p = hist2M.popleft()
                    sum2 -= p

                cutoff10 = now_ms - 600_000
                while hist10 and hist10[0][0] < cutoff10:
                    _, p = hist10.popleft()
                    sum10 -= p

                cutoff30 = now_ms - 1_800_000
                while hist30 and hist30[0][0] < cutoff30:
                    _, p = hist30.popleft()
                    sum30 -= p

                cutoff1h = now_ms - 3_600_000
                while hist1h and hist1h[0][0] < cutoff1h:
                    _, p = hist1h.popleft()
                    sum1h -= p

                avg2M = (sum2 / len(hist2M)) if hist2M else ref_price
                avg10M = (sum10 / len(hist10)) if hist10 else ref_price
                avg30M = (sum30 / len(hist30)) if hist30 else ref_price
                avg1H = (sum1h / len(hist1h)) if hist1h else ref_price

                # gain / pnl "live" — recalculé à CHAQUE tick, en mémoire, sans I/O.
                # Sert aux décisions de seuil/target/hedge/clôture ci-dessous ; l'écriture
                # en DB reste throttlée plus bas (section 7) pour ne pas spammer la base.

                sB, sC = size_B or 0.0, size_C or 0.0
                total_units_full = size_A + sB + sC                      # taille ORIGINALE, référence fixe
                total_units = total_units_full * remaining_fraction      # exposition réellement encore ouverte

                # valorisation "à la sortie" : prix moyen pondéré qu'on obtiendrait
                # RÉELLEMENT en vendant/achetant total_units MAINTENANT contre le
                # carnet courant (impact de marché inclus), pas juste le meilleur prix affiché.
                exit_levels = bids if is_long else asks
                exit_vwap = self._theoretical_exit_vwap(exit_levels, total_units, ref_price)
                value_now = total_units * exit_vwap

                floating_hedge_gain = 0.0
                if status == "hedged" and hedge_entry_price is not None:
                    hedge_exit_levels = bids if hedge_side == "LONG" else asks
                    hedge_fallback = bid if hedge_side == "LONG" else ask
                    hedge_exit_price = self._theoretical_exit_vwap(hedge_exit_levels, hedge_size, hedge_fallback)
                    hsign = 1 if hedge_side == "LONG" else -1
                    floating_hedge_gain = hsign * hedge_size * (hedge_exit_price - hedge_entry_price) - hedge_fees

                if is_long:
                    aB, aC = ask_B or 0.0, ask_C or 0.0
                    invested_full = size_A * ask_A + sB * aB + sC * aC
                    invested = invested_full * remaining_fraction
                    gain_live = (value_now - invested) - total_fees + realized_hedge_gain + floating_hedge_gain + hedge_realized_partial_gain + realized_partial_gain
                else:
                    bB, bC = bid_B or 0.0, bid_C or 0.0
                    invested_full = size_A * bid_A + sB * bB + sC * bC
                    invested = invested_full * remaining_fraction
                    gain_live = (invested - value_now) - total_fees + realized_hedge_gain + floating_hedge_gain + hedge_realized_partial_gain + realized_partial_gain

                # dénominateur = capital ORIGINAL engagé (pas la fraction restante) : le pnl%
                # reste stable et interprétable même quand remaining_fraction devient petit,
                # et reflète la performance totale (réalisée + latente) sur le capital initial.
                pnl_live = gain_live / invested_full if invested_full else 0.0

                # suivi de progression du gain, indépendant du throttle d'écriture DB
                # (condition de clôture n°4 : stagnation)
                if top_pnl is None or pnl_live > top_pnl + Config.GAIN_PROGRESS_EPSILON:
                    last_progress_time = now_ms

                structural = False

                # 0bis) sorties partielles (scale-out) : clôture progressive d'une fraction de la
                # taille ORIGINALE quand pnl_live franchit un palier de PARTIAL_EXIT_LEVELS.
                # Restreint à status == "active" : pendant un hedge, la taille couverte (hedge_size)
                # est figée à l'ouverture -- réduire total_units sans réajuster le hedge romprait le
                # ratio de couverture et pourrait inverser l'exposition nette. On attend la sortie du
                # hedge (retour à "active") avant de réévaluer les paliers de sortie partielle.
                if status == "active" and remaining_fraction > 0 and nb_partial_exit < len(Config.PARTIAL_EXIT_LEVELS):
                    threshold, close_frac_target = Config.PARTIAL_EXIT_LEVELS[nb_partial_exit]
                    if pnl_live >= threshold and close_frac_target > 0:
                        close_frac_target = min(close_frac_target, remaining_fraction)  # garde-fou
                        target_units_raw = total_units_full * close_frac_target
                        target_units = self._round_size(symbol, target_units_raw)
                        order_side = "sell" if is_long else "buy"
                        exit_ref_price = bid if is_long else ask

                        if target_units <= 0:
                            # Palier atteint mais la fraction visée, une fois arrondie au pas du
                            # marché, tombe à 0 (position trop petite / palier trop fin) -- on ne
                            # peut pas exécuter ce scale-out. On NE marque PAS nb_partial_exit comme
                            # franchi : pnl_live restant >= threshold, on retentera au prochain tick,
                            # sans effet tant que la situation ne change pas (pas de boucle infinie
                            # coûteuse, juste un no-op silencieux comme pour les apports).
                            logging.info(
                                f"Sortie partielle {nb_partial_exit + 1} ignorée sur {symbol} "
                                f"(position {id_pos}) — fraction visée sous le minimum de marché "
                                f"après arrondi ({target_units_raw:.8f} -> 0)."
                            )
                        else:
                            filled_size, vwap_price, fill_status = await self._execute_fill_order(
                                symbol, order_side, target_units, exit_ref_price, id_pos, kind="partial_exit"
                            )

                            if fill_status == "aborted":
                                logging.info(
                                    f"Sortie partielle {nb_partial_exit + 1} abandonnée sur {symbol} "
                                    f"(position {id_pos}) — pas assez de liquidité dans la tolérance de "
                                    f"slippage, nouvelle tentative au prochain tick."
                                )
                            else:
                                # 'done' ou 'partial' -- même convention que les apports A/B/C : on
                                # accepte ce qui a réellement pu être rempli plutôt que de bloquer
                                # tout le palier tant que la totalité de la cible n'est pas atteinte.
                                close_frac = min(filled_size / total_units_full, remaining_fraction)
                                close_price = vwap_price
                                value_closed = filled_size * close_price
                                invested_closed = invested_full * close_frac
                                exit_fee = filled_size * close_price * Config.FEE_RATE
                                entry_fee_portion = total_fees * close_frac

                                if is_long:
                                    slice_gain = (value_closed - invested_closed) - entry_fee_portion - exit_fee
                                else:
                                    slice_gain = (invested_closed - value_closed) - entry_fee_portion - exit_fee

                                realized_partial_gain += slice_gain
                                total_fees -= entry_fee_portion
                                total_fees_paid += exit_fee
                                remaining_fraction -= close_frac
                                nb_partial_exit += 1

                                # variables locales resynchronisées pour le reste de CE tick
                                total_units = total_units_full * remaining_fraction
                                invested = invested_full * remaining_fraction

                                pending.update(
                                    remaining_fraction=remaining_fraction,
                                    realized_partial_gain=realized_partial_gain,
                                    nb_partial_exit=nb_partial_exit,
                                    total_fees=total_fees,
                                    total_fees_paid=total_fees_paid,
                                )
                                structural = True
                                logging.info(
                                    f"Sortie partielle {nb_partial_exit} sur la position {id_pos} ({symbol}) : "
                                    f"{close_frac:.2%} clôturés @ VWAP={close_price:.6f} "
                                    f"(cible={close_frac_target:.0%}, statut={fill_status}, pnl={pnl_live:.2%}), "
                                    f"gain réalisé sur la tranche={slice_gain:.4f}."
                                )

                # 1) premier apport
                if Config.ALLOW_DCA and status == "active" and nb_apport == 0 and (avg10M <= seuil_apport_low or avg10M >= seuil_apport_high):
                    order_side = "buy" if is_long else "sell"
                    apport_ref_price = ask if is_long else bid
                    target_size_B_raw = (Config.POSITION_USD * Config.FRACTION_DCA) / apport_ref_price
                    target_size_B = self._round_size(symbol, target_size_B_raw)
                    if target_size_B <= 0:
                        logging.info(
                            f"1er apport sur {symbol} (position {id_pos}) sous le minimum de "
                            f"marché après arrondi, apport ignoré ce tick."
                        )
                        target_size_B = 0.0  # évite d'exécuter le bloc ci-dessous
                    if target_size_B > 0:
                        filled_B, vwap_B, fill_status_B = await self._execute_fill_order(
                            symbol, order_side, target_size_B, apport_ref_price, id_pos, kind="open_B"
                        )
                        if fill_status_B == "aborted":
                            logging.info(
                                f"1er apport abandonné sur {symbol} (position {id_pos}) — "
                                f"pas assez de liquidité dans la tolérance de slippage, nouvelle tentative au prochain tick."
                            )
                        else:
                            ticker_after = await self.exchange.fetch_ticker(symbol)
                            ask_now = ticker_after.get("ask", ask)
                            bid_now = ticker_after.get("bid", bid)

                            size_B = filled_B
                            ask_B = vwap_B if is_long else ask_now
                            bid_B = bid_now if is_long else vwap_B
                            fee_B = size_B * vwap_B * Config.FEE_RATE
                            total_fees += fee_B
                            total_fees_paid += fee_B
                            nb_apport += 1
                            pending.update(
                                ask_B=ask_B, bid_B=bid_B, openTime_B=now_ms, size_B=size_B,
                                nb_apport=nb_apport, total_fees=total_fees, total_fees_paid=total_fees_paid,
                            )
                            structural = True
                            logging.info(
                                f"1er apport sur {symbol} (position {id_pos}) : "
                                f"{size_B:.6f} @ VWAP={vwap_B:.6f} (cible={target_size_B:.6f}, statut={fill_status_B})."
                            )

                # 2) deuxième apport
                elif Config.ALLOW_DCA and status == "active" and nb_apport == 1 and (avg10M <= seuil_apport_lower or avg10M >= seuil_apport_higher):
                    order_side = "buy" if is_long else "sell"
                    apport_ref_price = ask if is_long else bid
                    target_size_C_raw = (Config.POSITION_USD * Config.FRACTION_DCA) / apport_ref_price
                    target_size_C = self._round_size(symbol, target_size_C_raw)
                    if target_size_C <= 0:
                        logging.info(
                            f"2e apport sur {symbol} (position {id_pos}) sous le minimum de "
                            f"marché après arrondi, apport ignoré ce tick."
                        )
                    if target_size_C > 0:
                        filled_C, vwap_C, fill_status_C = await self._execute_fill_order(
                            symbol, order_side, target_size_C, apport_ref_price, id_pos, kind="open_C"
                        )
                        if fill_status_C == "aborted":
                            logging.info(
                                f"2e apport abandonné sur {symbol} (position {id_pos}) — "
                                f"pas assez de liquidité dans la tolérance de slippage, nouvelle tentative au prochain tick."
                            )
                        else:
                            ticker_after = await self.exchange.fetch_ticker(symbol)
                            ask_now = ticker_after.get("ask", ask)
                            bid_now = ticker_after.get("bid", bid)

                            size_C = filled_C
                            ask_C = vwap_C if is_long else ask_now
                            bid_C = bid_now if is_long else vwap_C
                            fee_C = size_C * vwap_C * Config.FEE_RATE
                            total_fees += fee_C
                            total_fees_paid += fee_C
                            nb_apport += 1
                            pending.update(
                                ask_C=ask_C, bid_C=bid_C, openTime_C=now_ms, size_C=size_C,
                                nb_apport=nb_apport, total_fees=total_fees, total_fees_paid=total_fees_paid,
                            )
                            structural = True
                            logging.info(
                                f"2e apport sur {symbol} (position {id_pos}) : "
                                f"{size_C:.6f} @ VWAP={vwap_C:.6f} (cible={target_size_C:.6f}, statut={fill_status_C})."
                            )

                # 3) seuil de hedge (ratchet), offset dynamique
                atr_pct = (atr / ref_price) if (atr and ref_price) else 0.0

                # momentum en cascade sur 3 échelles : accélération immédiate (2m/10m),
                # signal principal (10m/30m), tendance de fond (30m/1h)
                mom_fast = ((avg2M - avg10M) / avg10M) if avg10M else 0.0
                mom_mid = ((avg10M - avg30M) / avg30M) if avg30M else 0.0
                mom_slow = ((avg30M - avg1H) / avg1H) if avg1H else 0.0
                if not is_long:
                    mom_fast, mom_mid, mom_slow = -mom_fast, -mom_mid, -mom_slow

                # atténue le signal si les échelles de temps ne sont pas alignées
                # (spike récent isolé = probablement du bruit, pas une vraie tendance)
                aligned = sum(1 for m in (mom_fast, mom_mid, mom_slow) if m > 0)
                alignment_factor = 0.5 + 0.5 * (aligned / 3)

                momentum_norm = (mom_mid / atr_pct) if atr_pct else 0.0
                momentum_score = _clamp(momentum_norm / Config.MOMENTUM_FULL_TIGHTEN, 0.0, 1.0) * alignment_factor

                # dès qu'on est gagnant, même faiblement, on commence à sécuriser
                pnl_score = _clamp(pnl_live / Config.PNL_FULL_TIGHTEN, 0.0, 1.0)

                tightness = max(pnl_score, momentum_score)

                # tendance essoufflée (chute du score depuis le pic) : on force le
                # resserrement maximal du seuil de hedge, indépendamment du pnl/momentum
                if trend_exhausted:
                    tightness = 1.0

                # chaque hedge déjà réalisé sur cette position élargit la marge,
                # pour éviter le thrashing et l'accumulation de frais
                fatigue = 1 + Config.HEDGE_FATIGUE_STEP * nb_hedge

                offset_pct = (Config.OFFSET_LOOSE - tightness * (Config.OFFSET_LOOSE - Config.OFFSET_TIGHT)) * fatigue
                # l'offset ne descend jamais sous un multiple du spread courant : sinon
                # le seuil se ferait toucher par le bruit du spread, sans mouvement réel
                offset_pct = max(offset_pct, Config.MIN_OFFSET_SPREAD_MULT * spread_perc)

                factor_s = (1 - offset_pct + spread_perc) if is_long else (1 + offset_pct - spread_perc)
                seuil_current = avg10M * factor_s
                if seuil is None or (seuil_current >= seuil if is_long else seuil_current <= seuil):
                    seuil = seuil_current
                    pending["seuil"] = seuil

                # 4) entrée en hedge
                if status == "active":
                    is_entry = (avg2M <= seuil) if is_long else (avg2M >= seuil)
                    cooldown_ok = (
                        last_hedge_exit_time is None
                        or (now_ms - last_hedge_exit_time) >= Config.HEDGE_COOLDOWN_MS
                    )
                    if is_entry and cooldown_ok:
                        # hedge PARTIEL : on ne couvre qu'une fraction de l'exposition restante,
                        # pas 100% -- réduit le coût de couverture tout en captant une partie du
                        # rebond si la tendance repart, au prix d'une protection moindre.
                        # Arrondi vers le bas au pas de quantité réel du marché.
                        hedge_target_size_raw = total_units * Config.HEDGE_PARTIAL_FRACTION
                        hedge_target_size = self._round_size(symbol, hedge_target_size_raw)

                        # coût d'un aller-retour de hedge : 2x frais + ~1x spread, calculé sur la
                        # taille RÉELLEMENT hedgée (pas total_units), après arrondi
                        round_trip_cost = hedge_target_size * ref_price * (2 * Config.FEE_RATE + spread_perc)

                        if hedge_target_size <= 0: # or gain_live < Config.HEDGE_MIN_GAIN_MULTIPLE * round_trip_cost: #hedge que si peut couvrir les frais si activer
                            # le gain à sécuriser ne couvre pas le coût du hedge : on ne
                            # paie pas de frais pour rien, seuil continue de vivre, on
                            # retentera au prochain tick si la situation s'améliore
                            pass
                        else:
                            id_hedge_new, hs, hsize, hentry, hfee, succes = await self.W_openHedgePos(
                                id_pos, symbol, side, hedge_target_size, ask, bid
                            )
                            if succes:
                                status = "hedged"
                                id_hedge = id_hedge_new
                                hedge_side, hedge_size, hedge_entry_price, hedge_fees = hs, hsize, hentry, hfee
                                hedge_open_time = now_ms
                                hedge_original_size = hsize
                                hedge_nb_partial_exit = 0
                                hedge_realized_partial_gain = 0.0 
                                nb_hedge += 1  # fix : la variable locale n'était jamais incrémentée
                                pending["status"] = status
                                pending["id_hedge"] = id_hedge
                                pending["nb_hedge"] = nb_hedge

                                recovery_pct = (atr_pct * Config.RECOVERY_ATR_MULT / fatigue) if atr_pct else Config.OFFSET_LOOSE
                                if trend_exhausted:
                                    # tendance essoufflée : on rapproche la target du seuil pour sortir
                                    # du hedge plus vite et repasser en 'active' dès qu'une reprise même
                                    # modeste se dessine, plutôt que d'attendre une recovery complète.
                                    recovery_pct *= Config.TREND_EXHAUSTED_RECOVERY_MULT
                                recovery_pct = max(recovery_pct, Config.MIN_RECOVERY_SPREAD_MULT * spread_perc)
                                depth_adj = Config.RECOVERY_DEEP_BONUS if pnl_live < -0.01 else 0.0

                                if is_long:
                                    factor_r = 1 + recovery_pct + depth_adj + spread_perc
                                else:
                                    factor_r = 1 - recovery_pct - depth_adj - spread_perc
                                target = avg10M * factor_r

                                # --- Plafond de l'écart seuil/target (multiples d'ATR) : évite qu'un
                                # ATR élevé ne fixe un target trop loin du seuil, ce qui garderait le
                                # hedge ouvert indéfiniment.
                                max_gap = Config.HEDGE_TARGET_MAX_GAP_ATR_MULT * atr
                                if max_gap > 0:
                                    if is_long:
                                        target = min(target, seuil + max_gap)
                                    else:
                                        target = max(target, seuil - max_gap)

                                # --- Garantie de breakeven, généralisée au hedge PARTIEL ---
                                # true_gain(P) = gain combiné (position restante + hedge) si le hedge
                                # est clôturé au prix P, frais de clôture du hedge inclus (pas encore
                                # payés à cet instant, donc pas dans floating_hedge_gain).
                                # Ne s'applique QUE si le hedge est réellement partiel : si
                                # hsize == total_units (hedge complet, cas actuel avec
                                # HEDGE_PARTIAL_FRACTION = 1), coef se réduit à -hsize*FEE_RATE
                                # (quasi nul) -> price_breakeven diverge et donne un target
                                # aberrant. On saute le clip dans ce cas ; recovery_pct suffit.
                                residual = total_units - hsize
                                if abs(residual) > 1e-9:
                                    if is_long:
                                        coef = residual - hsize * Config.FEE_RATE
                                        const_term = hsize * hentry - hfee - invested - total_fees + realized_hedge_gain + realized_partial_gain
                                    else:
                                        coef = -residual - hsize * Config.FEE_RATE
                                        const_term = invested - hsize * hentry - hfee - total_fees + realized_hedge_gain + realized_partial_gain

                                    if coef != 0:
                                        price_breakeven = -const_term / coef
                                        if coef > 0:
                                            target = max(target, price_breakeven)
                                        else:
                                            target = min(target, price_breakeven)

                                        # re-clip : le breakeven ne doit jamais repousser la
                                        # target hors de la plage déjà validée par max_gap.
                                        if max_gap > 0:
                                            if is_long:
                                                target = min(target, seuil + max_gap)
                                            else:
                                                target = max(target, seuil - max_gap)

                                pending["target"] = target
                                structural = True
                            else:
                                logging.error(f"W_gestionPos: échec ouverture hedge sur {id_pos} ({symbol}), nouvelle tentative au prochain tick.")

                # 5) sorties paliers du hedge (entre seuil et target), puis sortie totale
                elif status == "hedged" and target is not None:
                    span = target - seuil
                    progress = _clamp((avg2M - seuil) / span, 0.0, 1.0) if span else 0.0

                    if hedge_nb_partial_exit < len(Config.HEDGE_EXIT_LEVELS):
                        threshold, cum_frac_target = Config.HEDGE_EXIT_LEVELS[hedge_nb_partial_exit]
                        if progress >= threshold:
                            prev_cum_frac = (
                                Config.HEDGE_EXIT_LEVELS[hedge_nb_partial_exit - 1][1]
                                if hedge_nb_partial_exit > 0 else 0.0
                            )
                            close_frac_incr = cum_frac_target - prev_cum_frac
                            target_units_raw = (hedge_original_size or 0.0) * close_frac_incr
                            target_units = self._round_size(symbol, target_units_raw)
                            order_side = "sell" if hedge_side == "LONG" else "buy"
                            exit_ref_price = bid if hedge_side == "LONG" else ask

                            if target_units <= 0:
                                # même convention que le scale-out côté position : palier
                                # atteint mais fraction visée sous le pas de marché après
                                # arrondi -- on ne marque pas le palier franchi, nouvelle
                                # tentative silencieuse au prochain tick.
                                logging.info(
                                    f"Sortie palier hedge {hedge_nb_partial_exit + 1} ignorée sur {symbol} "
                                    f"(position {id_pos}) — fraction visée sous le minimum de marché "
                                    f"après arrondi ({target_units_raw:.8f} -> 0)."
                                )
                            else:
                                filled_size, vwap_price, fill_status = await self._execute_fill_order(
                                    symbol, order_side, target_units, exit_ref_price, id_pos,
                                    kind="hedge_partial_exit", id_hedge=id_hedge,
                                )

                                if fill_status == "aborted":
                                    logging.info(
                                        f"Sortie palier hedge {hedge_nb_partial_exit + 1} abandonnée sur "
                                        f"{symbol} (position {id_pos}) — pas assez de liquidité dans la "
                                        f"tolérance de slippage, nouvelle tentative au prochain tick."
                                    )
                                else:
                                    close_fee = filled_size * vwap_price * Config.FEE_RATE
                                    # part du pool de frais d'entrée du hedge attribuable à cette
                                    # tranche -- même logique que le scale-out côté position :
                                    # fraction calculée sur la taille ORIGINALE, appliquée au pool
                                    # de frais RESTANT (déjà réduit par d'éventuels paliers précédents).
                                    close_frac = filled_size / hedge_original_size if hedge_original_size else 0.0
                                    entry_fee_portion = hedge_fees * close_frac
                                    hsign = 1 if hedge_side == "LONG" else -1
                                    slice_gain = (
                                        hsign * filled_size * (vwap_price - hedge_entry_price)
                                        - entry_fee_portion - close_fee
                                    )

                                    hedge_realized_partial_gain += slice_gain
                                    hedge_fees -= entry_fee_portion
                                    hedge_size -= filled_size
                                    hedge_nb_partial_exit += 1

                                    await self.db.update_hedge(
                                        id_hedge,
                                        size=hedge_size,
                                        total_fees=hedge_fees,
                                        realized_partial_gain=hedge_realized_partial_gain,
                                        nb_partial_exit=hedge_nb_partial_exit,
                                    )
                                    structural = True
                                    logging.info(
                                        f"Sortie palier hedge {hedge_nb_partial_exit} sur la position {id_pos} "
                                        f"({symbol}) : {filled_size:.6f} clôturés @ VWAP={vwap_price:.6f} "
                                        f"(progress={progress:.2%}, cible cumulée={cum_frac_target:.0%}, "
                                        f"statut={fill_status}), gain tranche={slice_gain:.4f}."
                                    )

                    is_exit = (avg10M >= target) if is_long else (avg10M <= target)
                    if is_exit:
                        hedge_gain, fully_closed = await self.W_closeHedgePos(id_hedge, ask, bid)
                        if fully_closed:
                            # hedge_gain inclut ici TOUT le cycle (paliers + clôture finale) --
                            # cf. réécriture de W_closeHedgePos. Seul le cas fully_closed doit
                            # alimenter realized_hedge_gain (qui reflète les hedges 'closed' en
                            # DB) -- sinon double comptage avec hedge_realized_partial_gain.
                            realized_hedge_gain += hedge_gain
                            status, target = "active", None
                            hedge_open_time = None
                            last_hedge_exit_time = now_ms
                            id_hedge = None
                            hedge_original_size = None
                            hedge_nb_partial_exit = 0
                            hedge_realized_partial_gain = 0.0
                            pending.update(status=status, target=None, id_hedge=None)
                        else:
                            # reliquat conservé en hedge -- on resynchronise l'état local
                            # (taille, frais, gain paliers déjà persistés par W_closeHedgePos)
                            hedge_refresh = await self.db.get_hedge(id_hedge)
                            if hedge_refresh:
                                hedge_size = hedge_refresh["size"] or 0.0
                                hedge_fees = hedge_refresh["total_fees"] or 0.0
                                hedge_realized_partial_gain = hedge_refresh["realized_partial_gain"] or 0.0
                        structural = True

                # 6) réévaluation du score, une fois par heure
                if now_ms - last_score_update >= Config.SLEEP_INTERVAL * 1000:
                    entry_price = ask_A if is_long else bid_A
                    new_score = await compute_score(
                        (symbol, pos["openTime_A"], side, entry_price), self.exchange, live_pnl=pnl_live
                    )
                    if new_score is not None:
                        pending["score"] = new_score

                        # purge des lectures sorties de la fenêtre de SCORE_ESSOUFFLEMENT_WINDOW
                        # heures (comparaison AVANT ajout de new_score : "avant" vs "maintenant")
                        cutoff_score = now_ms - score_window_ms
                        while score_history and score_history[0][0] < cutoff_score:
                            score_history.popleft()

                        # condition de clôture n°0 : essoufflement -- le score actuel a
                        # chuté d'au moins SCORE_ESSOUFFLEMENT_DROP par rapport au pic
                        # mémorisé (score_var) sur la fenêtre glissante
                        if score_history:
                            score_peak = max(s for _, s in score_history)
                            if score_peak > 0 and (score_peak - new_score) >= Config.SCORE_ESSOUFFLEMENT_DROP:
                                trend_exhausted = True

                        score_history.append((now_ms, new_score))

                        # persistance du pic de la fenêtre : mémoire pour la comparaison
                        # ci-dessus au prochain cycle ET graine de reprise après un
                        # redémarrage -- même UPDATE que 'score', aucun coût DB additionnel
                        pending["score_var"] = max(s for _, s in score_history)
                    else:
                        logging.info(f"Score non recalculé pour la position {id_pos} ({symbol}).")

                    new_atr = await self._fetch_atr(symbol)
                    if new_atr:
                        atr = new_atr
                        pending["atr"] = atr

                        # Recalcule les seuils d'apport non encore déclenchés avec l'ATR à jour,
                        # en gardant le prix d'entrée comme référence fixe (seule la largeur change).
                        entry_ref = ask_A if is_long else bid_A
                        if nb_apport == 0:
                            seuil_apport_low = entry_ref - 1.1 * atr
                            seuil_apport_high = entry_ref + 1.1 * atr
                            pending["seuil_apport_low"] = seuil_apport_low
                            pending["seuil_apport_high"] = seuil_apport_high
                        if nb_apport <= 1:
                            seuil_apport_lower = entry_ref - 2.2 * atr
                            seuil_apport_higher = entry_ref + 2.2 * atr
                            pending["seuil_apport_lower"] = seuil_apport_lower
                            pending["seuil_apport_higher"] = seuil_apport_higher

                    last_score_update = now_ms
                    structural = True

                # 7) écriture DB throttlée — réutilise gain_live/pnl_live déjà calculés
                #    en tête de boucle, flushée immédiatement sur événement structurant
                due = structural or (now_ms - last_write >= Config.GESTION_MIN_WRITE_INTERVAL_MS)
                if due:
                    current_pnl = pnl_live
                    pending["gain"] = gain_live
                    pending["pnl"] = current_pnl
                    if top_pnl is None or current_pnl > top_pnl:
                        top_pnl = current_pnl
                        pending["top_pnl"] = top_pnl
                    if drawdown is None or current_pnl < drawdown:
                        drawdown = current_pnl
                        pending["drawdown"] = drawdown

                    if pending:
                        await self.db.update_position(id_pos, **pending)
                        pending = {}
                    last_write = now_ms

                # 8) fermeture automatique si l'une des conditions de clôture est remplie
                if status != "closed":
                    hedge_too_long = (
                        status == "hedged"
                        and hedge_open_time is not None
                        and now_ms - hedge_open_time >= Config.HEDGE_MAX_DURATION_MS
                    )
                    gain_stagnation = (
                        pnl_live > 0
                        and now_ms - last_progress_time >= Config.GAIN_STAGNATION_MS
                    )
                    close_reason = next(
                        (reason for reason, triggered in (
                            #("stop_loss",        pnl_live <= Config.STOP_LOSS_PNL),
                            ("hedge_too_long",   hedge_too_long),
                            ("max_duration",     now_ms - pos["openTime_A"] >= Config.POSITION_DURATION_LIMIT),
                            ("gain_stagnation",  gain_stagnation),
                            ("trend_exhaustion", trend_exhausted),# and status == "active"),#si trend morte meme en hedge < 0, on sort
                        ) if triggered),
                        None,
                    )
                    if close_reason:
                        succes = await self.W_closePos(id_pos, reason=close_reason, ask=ask, bid=bid)
                        if succes:
                            status = "closed"

                if status == "closed":
                    break

            except asyncio.CancelledError:
                raise

            except (ccxt.AuthenticationError, ccxt.PermissionDenied, ccxt.BadSymbol) as e:
                # Erreurs fatales : retenter ne résoudra rien (clé API invalide, permissions,
                # symbole delisté...). On arrête cette tâche de gestion et on alerte.
                # La position reste "active"/"hedged" en DB et sera reprise par
                # recover_active_positions() au prochain redémarrage, une fois le problème corrigé.
                await self._alert_critical(
                    f"W_gestionPos: erreur FATALE sur {symbol} (position {id_pos}), "
                    f"arrêt de la gestion : {e}"
                )
                return

            except (ccxt.NetworkError, ccxt.ExchangeNotAvailable, ccxt.OnMaintenance) as e:
                consecutive_failures += 1
                delay = min(
                    Config.WS_RETRY_BASE_DELAY_S * (2 ** (consecutive_failures - 1)),
                    Config.WS_RETRY_MAX_DELAY_S,
                )
                logging.warning(
                    f"W_gestionPos: erreur réseau sur {symbol} (position {id_pos}), "
                    f"tentative {consecutive_failures}, retry dans {delay}s : {e}"
                )
                await self._handle_ws_failure(id_pos, symbol, consecutive_failures)
                await asyncio.sleep(delay)

            except Exception as e:
                # Erreur non classifiée : traitée prudemment comme transitoire
                # (backoff + comptage) plutôt que de spammer en boucle à 1s fixe.
                consecutive_failures += 1
                delay = min(
                    Config.WS_RETRY_BASE_DELAY_S * (2 ** (consecutive_failures - 1)),
                    Config.WS_RETRY_MAX_DELAY_S,
                )
                logging.error(
                    f"W_gestionPos: erreur non classifiée sur {symbol} (position {id_pos}), "
                    f"tentative {consecutive_failures}, retry dans {delay}s : {e}"
                )
                await self._handle_ws_failure(id_pos, symbol, consecutive_failures)
                await asyncio.sleep(delay)

        logging.info(f"Gestion de la position {id_pos} terminée.")

    async def W_closePos(self, id_pos: int, reason: str = None, ask: float = None, bid: float = None) -> bool:
        """
        Ferme une position en simulant son remplissage contre le carnet réel
        (Option B). Un fill partiel réduit remaining_fraction et alimente
        realized_partial_gain (même mécanique que le scale-out existant) -- la
        position reste 'active'/'hedged' et sera retentée au tick suivant par
        la même condition de clôture (le close_reason redeviendra vrai).

        Le hedge actif n'est clôturé QUE lors de la clôture définitive
        (remaining_fraction -> 0), pour ne jamais laisser une exposition
        significative non couverte pendant qu'on retente de sortir le reste.

        Retourne True seulement si la position est intégralement clôturée et
        persistée en 'closed'.
        """
        pos = await self.db.get_position(id_pos)
        if pos is None:
            logging.error(f"W_closePos: position {id_pos} introuvable en DB.")
            return False

        symbol = pos["symbol"]
        side = pos["side"]
        is_long = side == "LONG"

        try:
            if ask is None or bid is None:
                ticker = await self.exchange.fetch_ticker(symbol)
                ask, bid = ticker.get("ask"), ticker.get("bid")
            if ask is None or bid is None:
                logging.error(f"W_closePos: ask/bid indisponible pour {symbol} (position {id_pos}).")
                return False

            total_fees = pos["total_fees"] or 0.0
            total_fees_paid = pos["total_fees_paid"] or 0.0
            remaining_fraction = pos["remaining_fraction"] if pos["remaining_fraction"] is not None else 1.0
            realized_partial_gain = pos["realized_partial_gain"] or 0.0
            size_A, size_B, size_C = pos["size_A"] or 0.0, pos["size_B"] or 0.0, pos["size_C"] or 0.0
            total_units_full = size_A + size_B + size_C
            total_units_raw = total_units_full * remaining_fraction
            # size_A/B/C ont déjà été arrondis au pas du marché à l'ouverture/apport :
            # cet arrondi ici ne fait que nettoyer un résidu flottant, sauf cas de
            # dust résiduel après plusieurs scale-outs (cf. remarque plus bas).
            total_units = self._round_size(symbol, total_units_raw)

            if is_long:
                ask_A, ask_B, ask_C = pos["ask_A"] or 0.0, pos["ask_B"] or 0.0, pos["ask_C"] or 0.0
                invested_full = size_A * ask_A + size_B * ask_B + size_C * ask_C
            else:
                bid_A, bid_B, bid_C = pos["bid_A"] or 0.0, pos["bid_B"] or 0.0, pos["bid_C"] or 0.0
                invested_full = size_A * bid_A + size_B * bid_B + size_C * bid_C

            close_price = None

            # Reliquat non nul mais sous le pas de quantité minimal du marché (dust) :
            # un vrai exchange rejetterait l'ordre, et retenter au tick suivant ne
            # changerait rien (le reliquat ne grandit pas tout seul) -- la position
            # resterait bloquée indéfiniment en "active"/"hedged". On l'acte donc
            # directement au prix courant, sans passage par le carnet ni frais de
            # sortie simulés (aucun ordre réel n'aurait pu être passé).
            is_dust = total_units_raw > 1e-12 and total_units <= 0

            if is_dust:
                dust_price = bid if is_long else ask
                value_closed = total_units_raw * dust_price
                invested_closed = invested_full * remaining_fraction
                entry_fee_portion = total_fees * remaining_fraction

                if is_long:
                    slice_gain = (value_closed - invested_closed) - entry_fee_portion
                else:
                    slice_gain = (invested_closed - value_closed) - entry_fee_portion

                realized_partial_gain += slice_gain
                total_fees -= entry_fee_portion
                close_price = dust_price
                remaining_fraction = 0.0

                logging.info(
                    f"W_closePos: reliquat dust sur {symbol} (position {id_pos}) — "
                    f"{total_units_raw:.8f} sous le minimum de marché, clôturé directement "
                    f"au prix courant sans passage par le carnet (gain reliquat={slice_gain:.6f})."
                )

            elif total_units > 1e-12:
                order_side = "sell" if is_long else "buy"
                ref_price = bid if is_long else ask

                filled_size, vwap_price, fill_status = await self._execute_fill_order(
                    symbol, order_side, total_units, ref_price, id_pos, kind="close"
                )

                if fill_status == "aborted" or filled_size <= 0:
                    logging.warning(
                        f"W_closePos: aucune liquidité suffisante pour clôturer {symbol} "
                        f"(position {id_pos}), nouvelle tentative au prochain tick."
                    )
                    return False

                close_price = vwap_price
                close_frac = min(filled_size / total_units_full, remaining_fraction)
                value_closed = filled_size * close_price
                invested_closed = invested_full * close_frac
                exit_fee = filled_size * close_price * Config.FEE_RATE
                entry_fee_portion = total_fees * close_frac

                if is_long:
                    slice_gain = (value_closed - invested_closed) - entry_fee_portion - exit_fee
                else:
                    slice_gain = (invested_closed - value_closed) - entry_fee_portion - exit_fee

                realized_partial_gain += slice_gain
                total_fees -= entry_fee_portion
                total_fees_paid += exit_fee
                remaining_fraction -= close_frac
                if fill_status == "partial":
                    logging.info(
                        f"W_closePos: clôture PARTIELLE sur {symbol} (position {id_pos}) "
                        f"— {filled_size:.6f}/{total_units:.6f} rempli, reliquat conservé actif."
                    )

            fully_closed = remaining_fraction <= 1e-9

            if not fully_closed:
                await self.db.update_position(
                    id_pos,
                    remaining_fraction=remaining_fraction,
                    realized_partial_gain=realized_partial_gain,
                    total_fees=total_fees,
                    total_fees_paid=total_fees_paid,
                )
                return False

            # -- clôture définitive à partir d'ici --
            hedge_gain = 0.0
            hedge_fully_closed = True
            if pos["status"] == "hedged" and pos["id_hedge"]:
                hedge_gain, hedge_fully_closed = await self.W_closeHedgePos(pos["id_hedge"], ask, bid)
                if not hedge_fully_closed:
                    await self._alert_critical(
                        f"W_closePos: clôture FORCÉE de la position {id_pos} ({symbol}) alors que son "
                        f"hedge {pos['id_hedge']} n'a pu être clôturé que partiellement (liquidité/slippage). "
                        f"Un reliquat de hedge reste actif en DB, orphelin de sa position parente fermée "
                        f"-- à surveiller/clôturer manuellement."
                    )

            # Somme de TOUS les hedges déjà 'closed' sur cette position (cycles
            # ratchet précédents INCLUS le hedge qu'on vient de fermer ci-dessus,
            # s'il a été intégralement clôturé -- W_closeHedgePos l'a déjà persisté
            # en 'closed' avant qu'on arrive ici).
            realized_hedge_gain_total = (await self.db.execute_read(
                "SELECT COALESCE(SUM(gain), 0) FROM hedges WHERE id_parent = ? AND status = 'closed'",
                (id_pos,),
            ))[0][0]

            if hedge_fully_closed:
                # hedge_gain est déjà inclus dans la SUM ci-dessus (statut 'closed'
                # déjà persisté) -- ne pas le rajouter, sinon double comptage.
                total_hedge_gain = realized_hedge_gain_total
            else:
                # hedge resté 'active' (clôture partielle) -- son gain réalisé sur
                # la portion clôturée n'est PAS dans la SUM (statut pas encore
                # 'closed'), on doit l'ajouter manuellement.
                total_hedge_gain = realized_hedge_gain_total + hedge_gain

            gain = realized_partial_gain + total_hedge_gain
            pnl = gain / invested_full if invested_full else 0.0

            await self.db.close_position(
                id_pos,
                close=close_price,
                closeTime=int(time.time() * 1000),
                gain=gain,
                pnl=pnl,
                total_fees=total_fees,
                total_fees_paid=total_fees_paid,
                remaining_fraction=0.0,
                realized_partial_gain=realized_partial_gain,
            )

            logging.info(
                f"Position {id_pos} ({symbol}, {side}) fermée"
                f"{f' [{reason}]' if reason else ''} @ {close_price} — gain={gain:.4f}, pnl={pnl:.4%}."
            )
            return True

        except Exception as e:
            logging.error(f"Erreur W_closePos sur {id_pos}: {e}")
            return False

    async def W_openHedgePos(self, id_pos: int, symbol: str, side: str, total_units: float, ask: float, bid: float):
        """
        Ouvre un hedge en simulant son remplissage contre le carnet réel (Option B).
        ask/bid : dernier tick connu au moment de la décision (utilisés comme prix
        de référence initial pour la tolérance de slippage -- pas comme prix de fill garanti).
        """
        if total_units <= 0:
            return None, None, None, None, None, False

        hedge_side = "SHORT" if side == "LONG" else "LONG"
        order_side = "buy" if hedge_side == "LONG" else "sell"
        ref_price = ask if hedge_side == "LONG" else bid

        filled_size, vwap_price, fill_status = await self._execute_fill_order(
            symbol, order_side, total_units, ref_price, id_pos, kind="hedge_open",
            check_exit_liquidity=True,
        )

        if fill_status == "aborted":
            logging.info(
                f"W_openHedgePos: remplissage abandonné sur {symbol} (position {id_pos}) "
                f"— pas assez de liquidité dans la tolérance de slippage."
            )
            return None, None, None, None, None, False

        if fill_status == "partial":
            logging.info(
                f"W_openHedgePos: remplissage partiel sur {symbol} (position {id_pos}) "
                f"— {filled_size:.6f}/{total_units:.6f} rempli, hedge ouvert avec ce qui a été rempli."
            )

        ticker_after = await self.exchange.fetch_ticker(symbol)
        ask_now = ticker_after.get("ask", ask)
        bid_now = ticker_after.get("bid", bid)

        entry_price = vwap_price
        fee = filled_size * vwap_price * Config.FEE_RATE
        now_ms = int(time.time() * 1000)

        ask_hedge = vwap_price if hedge_side == "LONG" else ask_now
        bid_hedge = bid_now if hedge_side == "LONG" else vwap_price

        id_hedge = await self.db.insert_hedge(
            id_parent=id_pos, symbol=symbol, side=hedge_side, status="active",
            ask=ask_hedge, bid=bid_hedge, openTime=now_ms, size=filled_size,
            original_size=filled_size, total_fees=fee,
        )
        return id_hedge, hedge_side, filled_size, entry_price, fee, True

    async def W_closeHedgePos(self, id_hedge: int, ask: float, bid: float):
        """
        Ferme réellement le hedge id_hedge en simulant son remplissage contre le
        carnet réel (Option B). Retourne (hedge_gain, fully_closed).

        hedge_gain :
          - si fully_closed=True  : gain TOTAL du cycle de hedge (paliers déjà
            sortis + clôture finale), frais d'entrée proportionnels inclus.
          - si fully_closed=False : gain de CETTE seule tranche de clôture
            (le cumul reste en DB via realized_partial_gain pour la prochaine
            tentative de clôture finale).

        fully_closed=False signifie qu'un reliquat reste ouvert (slippage/timeout
        pendant la clôture) -- la taille du hedge en DB est réduite à ce reliquat,
        son statut RESTE 'active'. C'est à l'appelant de ne PAS repasser la
        position en 'active' tant que fully_closed n'est pas True.

        Point d'entrée UNIQUE pour la fermeture d'un hedge -- appelée à la fois
        par W_gestionPos (sortie normale, target atteint) et par W_closePos
        (fermeture forcée de la position parente alors qu'un hedge est actif).
        Idempotente : si le hedge est déjà 'closed', retourne (son gain existant, True).
        """
        hedge = await self.db.get_hedge(id_hedge)
        if hedge is None:
            logging.error(f"W_closeHedgePos: hedge {id_hedge} introuvable en DB.")
            return 0.0, True
        if hedge["status"] == "closed":
            return hedge["gain"] or 0.0, True

        hedge_side = hedge["side"]
        hedge_size = hedge["size"] or 0.0
        hedge_entry_price = hedge["ask"] if hedge_side == "LONG" else hedge["bid"]
        hedge_original_size = hedge["original_size"] or hedge_size
        hedge_realized_partial_gain = hedge["realized_partial_gain"] or 0.0
        symbol = hedge["symbol"]
        id_pos = hedge["id_parent"]

        order_side = "sell" if hedge_side == "LONG" else "buy"
        ref_price = bid if hedge_side == "LONG" else ask

        filled_size, vwap_price, fill_status = await self._execute_fill_order(
            symbol, order_side, hedge_size, ref_price, id_pos, kind="hedge_close", id_hedge=id_hedge
        )

        if fill_status == "aborted" or filled_size <= 0:
            logging.warning(
                f"W_closeHedgePos: impossible de clôturer le hedge {id_hedge} sur {symbol} "
                f"— aucune liquidité suffisante dans la tolérance de slippage, hedge laissé actif tel quel."
            )
            return 0.0, False

        hedge_exit_price = vwap_price
        close_fee = filled_size * hedge_exit_price * Config.FEE_RATE

        # part du pool de frais d'entrée (non encore réalisé) attribuable à CETTE
        # tranche -- même logique que les sorties paliers. Corrige un bug existant :
        # le gain final ne soustrayait auparavant que close_fee, jamais les frais
        # d'entrée, contrairement au calcul "floating" utilisé en cours de vie.
        fee_pool = hedge["total_fees"] or 0.0
        entry_fee_portion = fee_pool * (filled_size / hedge_original_size) if hedge_original_size else fee_pool

        sign = 1 if hedge_side == "LONG" else -1
        slice_gain = sign * filled_size * (hedge_exit_price - hedge_entry_price) - entry_fee_portion - close_fee
        total_fees_remaining = fee_pool - entry_fee_portion

        if fill_status == "partial":
            remaining_size = hedge_size - filled_size
            new_realized_partial_gain = hedge_realized_partial_gain + slice_gain
            logging.warning(
                f"W_closeHedgePos: clôture PARTIELLE du hedge {id_hedge} sur {symbol} "
                f"— {filled_size:.6f}/{hedge_size:.6f} clôturé, reliquat {remaining_size:.6f} laissé actif."
            )
            await self.db.update_hedge(
                id_hedge, size=remaining_size, total_fees=total_fees_remaining,
                realized_partial_gain=new_realized_partial_gain,
            )
            return slice_gain, False

        total_gain = hedge_realized_partial_gain + slice_gain
        invested_hedge = hedge_original_size * hedge_entry_price if hedge_original_size else filled_size * hedge_entry_price
        hedge_pnl = total_gain / invested_hedge if invested_hedge else 0.0

        await self.db.update_hedge(
            id_hedge,
            status="closed",
            close=hedge_exit_price,
            closeTime=int(time.time() * 1000),
            gain=total_gain,
            pnl=hedge_pnl,
            total_fees=total_fees_remaining,
        )
        return total_gain, True