"""One low-priority reconciliation worker shared by all monitored groups."""

import asyncio
import json
import random
import threading
import time
import weakref
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from loguru import logger

from ..client.base import BackgroundBusy
from ..client.request import (
    ClanBattleBossHistoryRequest, ClanBattleBossInfoRequest, ClanBattleLogListRequest,
)
from ..client.response import (
    ClanBattleBossHistoryResponse, ClanBattleBossInfoResponse, ClanBattleLogListResponse,
)
from ..database.dal import pcr_sqla
from ..database.reconciliation import ReconciliationStore, encode
from ..util.auto_boss import clan_boss_info


def realm_for(client):
    # The CN client calls this attribute 'platfrom'; TW separates servers by prefix.
    platform = getattr(client, "platfrom", None)
    if platform is not None:
        return "cn:%s" % platform
    return "tw:%s" % getattr(client, "platform", "unknown")


class ReconciliationCoordinator:
    GLOBAL_INTERVAL = 5
    ACCOUNT_INTERVAL = 30
    MAX_PAGES = 200
    MAX_REPORTS = 10000
    MAX_PAGE_ROWS = 100
    DEADLINE = 86400

    def __init__(self, store, clock=time.time):
        self.store = store
        self.clock = clock
        self._groups = weakref.WeakValueDictionary()
        self._guard = threading.Lock()
        self._task = None
        self._last_request = 0
        self._account_due = {}
        self._next_cleanup = 0
        self._next_error_log = 0
        self._pages = OrderedDict()
        self._cache_bytes = 0
        self._cache_admission = OrderedDict()
        self._observations = OrderedDict()
        self._registrations = OrderedDict()
        self._idle_until = 0

    def submit(self, clan, lap, boss, max_hp=None, kill_key=None, needs_job=False):
        realm = realm_for(clan.client)
        key = (realm, clan.clan_id, clan.clan_battle_id, lap, boss)
        with self._guard:
            if key not in self._observations:
                if len(self._observations) >= 512:
                    return False
                self._observations[key] = dict(period=getattr(clan, "battle_period", 0),
                    max_hp=max_hp, groups=set(), kills=set(), needs_job=False)
            entry = self._observations[key]
            entry["groups"].add(clan.group_id)
            entry["max_hp"] = entry["max_hp"] or max_hp
            entry["needs_job"] |= needs_job
            if kill_key:
                entry["kills"].add(tuple(kill_key))
            return True

    async def flush_observations(self):
        # Foreground monitors submit only compact facts. All SQLite work happens
        # here, with a fixed batch budget independent of the number of groups.
        for _ in range(5):
            with self._guard:
                if not self._observations:
                    break
                key, entry = self._observations.popitem(last=False)
            realm, clan_id, battle_id, lap, boss = key
            try:
                if entry["max_hp"]:
                    await self.store.save_bosses(realm, clan_id, battle_id,
                        [SimpleNamespace(lap_num=lap, order_num=boss, max_hp=entry["max_hp"])])
                if entry["needs_job"]:
                    config_hp = clan_boss_info.get_boss_max(lap, boss) if realm.startswith("cn:") else None
                    groups = list(entry["groups"])
                    await self.store.enqueue(realm, clan_id, battle_id, entry["period"], lap, boss,
                        groups[:99], config_hp, next(iter(entry["kills"]), None))
                    if len(entry["kills"]) > 1:
                        await self.store.update("%s:%s:%s:%s:%s" % key,
                                                status="unresolved", reason="multiple_kills")
                    if len(groups) > 99:
                        entry["groups"] = set(groups[99:])
                        with self._guard:
                            newer = self._observations.pop(key, None)
                            if newer:
                                entry["groups"].update(newer["groups"])
                                entry["kills"].update(newer["kills"])
                            self._observations[key] = entry
            except Exception:
                with self._guard:
                    newer = self._observations.pop(key, None)
                    if newer:
                        entry["groups"].update(newer["groups"])
                        entry["kills"].update(newer["kills"])
                        entry["needs_job"] |= newer["needs_job"]
                    self._observations[key] = entry
                raise
        for _ in range(5):
            with self._guard:
                if not self._registrations:
                    break
                key, group = self._registrations.popitem(last=False)
            try:
                await self.store.subscribe(*key[:3], group)
            except Exception:
                with self._guard:
                    self._registrations[key] = group
                raise

    def offer_page(self, clan, page):
        """Reuse compact facts only, never retain units, names or account data."""
        if page.battle_list is None or len(page.battle_list) > self.MAX_PAGE_ROWS:
            return
        prefix = (realm_for(clan.client), clan.clan_id, clan.clan_battle_id)
        now = self.clock()
        with self._guard:
            # Normal pagination can return many pages in a burst. Cache at most
            # one page per clan per five seconds; misses use the bounded worker.
            if self._cache_admission.get(prefix, 0) > now:
                return
            self._cache_admission[prefix] = now + self.GLOBAL_INTERVAL
            self._cache_admission.move_to_end(prefix)
            while len(self._cache_admission) > 128:
                self._cache_admission.popitem(last=False)
        rows = [[r.target_viewer_id, r.battle_log_id, r.lap_num, r.order_num,
                 r.battle_end_time, r.total_damage] for r in page.battle_list]
        payload = encode(rows).encode("utf-8")
        if len(payload) > 256 * 1024:
            return
        # Duplicate pages replace one another; sequence numbers only order reuse.
        page_key = prefix + (payload,)
        with self._guard:
            old = self._pages.pop(page_key, None)
            if old:
                self._cache_bytes -= len(old[2])
            self._pages[page_key] = (time.time_ns(), now, payload)
            self._cache_bytes += len(payload)
            while self._cache_bytes > 8 * 1024 * 1024 or len(self._pages) > 128:
                _, item = self._pages.popitem(last=False)
                self._cache_bytes -= len(item[2])

    def cached_page(self, job):
        prefix = (job["realm"], job["clan_id"], job["battle_id"])
        with self._guard:
            for key, item in self._pages.items():
                seq, created, payload = item
                if key[:3] == prefix and seq > job["cached_seq"] and self.clock() - created <= 60:
                    rows = json.loads(payload)
                    return seq, [SimpleNamespace(target_viewer_id=r[0], battle_log_id=r[1],
                            lap_num=r[2], order_num=r[3], battle_end_time=r[4], total_damage=r[5]) for r in rows]
        return None

    def start(self):
        with self._guard:
            if self._task is None or self._task.done():
                self._task = asyncio.create_task(self.run())

    async def register(self, clan, top):
        with self._guard:
            self._groups[clan.group_id] = clan
            key = (realm_for(clan.client), clan.clan_id, clan.clan_battle_id, clan.group_id)
            if len(self._registrations) < 512 or key in self._registrations:
                self._registrations[key] = clan.group_id
        clan._recon_loop_num = clan.loop_num
        clan._recon_bosses = {}
        clan._recon_deferred = []
        await self.observe(clan, top, [])
        # Explicitly scoped recovery for the reported incident, not a historical sweep.
        month = datetime.fromtimestamp(self.clock(), timezone(timedelta(hours=8))).strftime("%Y-%m")
        if clan.group_id == 619935706 and month == "2026-10":
            await self.enqueue(clan, 27, 5, (1343423659518, 1793008591))

    async def enqueue(self, clan, lap, boss, kill_key=None):
        hp = getattr(clan, "_recon_bosses", {}).get((lap, boss))
        return self.submit(clan, lap, boss, max_hp=hp, kill_key=kill_key, needs_job=True)

    async def observe(self, clan, top, histories):
        previous = getattr(clan, "_recon_bosses", {})
        current = {(b.lap_num, b.order_num): b.max_hp for b in top.boss_info or []}
        if previous == current and not histories and not getattr(clan, "_recon_deferred", []):
            return
        if previous != current:
            accepted = all([self.submit(clan, lap, boss, max_hp=hp)
                            for (lap, boss), hp in current.items()])
        else:
            accepted = True
        deferred = []
        for history in getattr(clan, "_recon_deferred", []) + list(histories):
            if history.kill:
                if not await self.enqueue(clan, history.lap_num, history.order_num,
                                          (history.viewer_id, history.create_time)):
                    deferred.append(history)
        clan._recon_deferred = deferred[-64:]
        for lap, boss in previous:
            if any(b.order_num == boss and b.lap_num > lap for b in top.boss_info or []):
                accepted = await self.enqueue(clan, lap, boss) and accepted
        if accepted:
            clan._recon_bosses = current
        if (not accepted or deferred) and self.clock() >= self._next_error_log:
            logger.warning("伤害校验提交队列已满，延后接收；即时播报继续")
            self._next_error_log = self.clock() + 60

    def client_for(self, job):
        with self._guard:
            for clan in self._groups.values():
                if (clan.clan_id == job["clan_id"] and clan.clan_battle_id == job["battle_id"]
                        and realm_for(clan.client) == job["realm"] and clan.loop_check):
                    if clan.loop_num == clan._recon_loop_num:
                        return clan.client
        return None

    async def retry(self, job, reason):
        attempts = job["attempts"] + 1
        delays = (30, 120, 600, 1800)
        delay = delays[min(attempts - 1, len(delays) - 1)] * random.uniform(.8, 1.2)
        if reason != job["reason"]:
            logger.info("伤害校验 {} 暂缓: {}", job["job_key"], reason)
        await self.store.reset(job["job_key"], attempts=attempts,
                               reason=reason, due=self.clock() + delay)

    async def tick(self):
        now = self.clock()
        with self._guard:
            submitted = bool(self._observations or self._registrations)
        if now < self._idle_until and not submitted:
            return
        await self.flush_observations()
        if now >= self._next_cleanup:
            await self.store.cleanup(now)
            self._next_cleanup = now + 60
            self._account_due = {k: v for k, v in self._account_due.items() if v > now - 60}
        applied = await self.store.apply_one(now)
        if applied:
            if applied["applied"] == 1:
                with self._guard:
                    clan = self._groups.get(applied["group_id"])
                    if clan:
                        clan.dao_update_time = int(now)
            elif applied["applied"] == -1:
                logger.warning("伤害校验 {} 群 {} 未写入：记录缺失、歧义或与已有修正冲突",
                               applied["job_key"], applied["group_id"])
        job = await self.store.due(now)
        if not job:
            self._idle_until = now + (self.GLOBAL_INTERVAL if applied else 30)
            return
        if now - job["created"] >= self.DEADLINE:
            await self.store.update(job["job_key"], status="unresolved", reason=job["reason"] or "expired")
            logger.warning("伤害校验 {} 已过期: {}", job["job_key"], job["reason"])
            return
        if job["due"] > now:
            self._idle_until = min(job["due"], now + 30)
            return
        self._idle_until = 0
        if job["stage"] == "reset":
            await self.store.reset(job["job_key"], due=now + self.GLOBAL_INTERVAL)
            return
        client = self.client_for(job)
        if client is None:
            await self.store.update(job["job_key"], due=now + 30)
            return
        if job["stage"] == "reports":
            cached = self.cached_page(job)
            if cached:
                seq, records = cached
                try:
                    count = await self.store.add_page(job, records)
                    if count > job["expected"]:
                        raise ValueError("attack_count_mismatch")
                    await self.store.update(job["job_key"], cached_seq=seq,
                        stage="after" if count == job["expected"] else "reports",
                        due=now + self.GLOBAL_INTERVAL)
                except ValueError as exc:
                    await self.retry(job, str(exc))
                return
        account = (job["realm"], client.viewer_id)
        due = max(self._last_request + self.GLOBAL_INTERVAL, self._account_due.get(account, 0))
        if due > now:
            await self.store.update(job["job_key"], due=due)
            return
        # Reserve before awaiting: a slow response must never cause a request burst.
        self._last_request = now
        self._account_due[account] = now + self.ACCOUNT_INTERVAL
        try:
            await self.step(job, client)
        except BackgroundBusy:
            await self.store.update(job["job_key"], due=now + self.GLOBAL_INTERVAL)
        except (ValueError, asyncio.TimeoutError) as exc:
            await self.retry(job, str(exc) or "request_timeout")
        except Exception as exc:
            # Do not persist credentials, headers, or raw API exception payloads.
            await self.retry(job, type(exc).__name__)

    async def step(self, job, client):
        args = dict(clan_id=job["clan_id"], clan_battle_id=job["battle_id"])
        key = job["job_key"]
        if job["stage"] in ("before", "after"):
            response = ClanBattleBossInfoResponse.parse_obj(await client.reconciliation_call(
                ClanBattleBossInfoRequest(**args, lap_num=job["lap"], order_num=job["boss"])))
            if response.current_hp != 0 or not isinstance(response.attack_count, int) or response.attack_count <= 0:
                raise ValueError("unconfirmed_kill_or_attack_count")
            if response.attack_count > self.MAX_REPORTS:
                await self.store.update(key, status="unresolved", reason="report_limit")
                return
            if job["stage"] == "before":
                await self.store.update(key, expected=response.attack_count, stage="history", page=1,
                                        due=self.clock() + self.GLOBAL_INTERVAL)
            else:
                if response.attack_count != job["expected"]:
                    raise ValueError("attack_count_changed")
                result = await self.store.evaluate(job, response.damage_history or [])
                await self.store.update(key, status="complete", result=encode(result), reason="",
                                        due=self.clock())
                logger.info("伤害校验 {} 完成：原始 {}，有效 {}", key, result["raw_damage"], result["damage"])
        elif job["stage"] == "history":
            response = ClanBattleBossHistoryResponse.parse_obj(await client.reconciliation_call(
                ClanBattleBossHistoryRequest(**args, period=job["period"], page=job["page"])))
            entries = response.boss_history or []
            if len(entries) > self.MAX_PAGE_ROWS:
                raise ValueError("oversized_page")
            matches = [h for h in entries if h.lap_num == job["lap"] and h.order_num == job["boss"]]
            if len(matches) > 1:
                raise ValueError("ambiguous_boss_history")
            if matches:
                history = matches[0]
                if (history.attack_count != job["expected"] or not isinstance(history.clear_time, int)
                        or history.clear_time <= 0):
                    raise ValueError("boss_history_mismatch")
                await self.store.update(key, clear_time=history.clear_time, stage="reports", page=1,
                                        due=self.clock() + self.GLOBAL_INTERVAL)
            elif not entries or job["page"] >= self.MAX_PAGES:
                raise ValueError("missing_boss_history")
            else:
                await self.store.update(key, page=job["page"] + 1, due=self.clock() + self.GLOBAL_INTERVAL)
        elif job["stage"] == "reports":
            response = ClanBattleLogListResponse.parse_obj(await client.reconciliation_call(
                ClanBattleLogListRequest(clan_battle_id=job["battle_id"], order_num=job["boss"], page=job["page"])))
            records = response.battle_list
            if records is None or len(records) > self.MAX_PAGE_ROWS:
                raise ValueError("missing_or_oversized_page")
            count = await self.store.add_page(job, records)
            if count > job["expected"]:
                raise ValueError("attack_count_mismatch")
            if count == job["expected"]:
                await self.store.update(key, stage="after", due=self.clock() + self.GLOBAL_INTERVAL)
            elif not records or not isinstance(response.max_page, int) or job["page"] >= response.max_page:
                raise ValueError("incomplete_reports")
            elif job["page"] >= self.MAX_PAGES:
                await self.store.update(key, status="unresolved", reason="page_limit")
            else:
                await self.store.update(key, page=job["page"] + 1, due=self.clock() + self.GLOBAL_INTERVAL)
        else:
            raise ValueError("unknown_stage")

    async def run(self):
        while True:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self.clock() >= self._next_error_log:
                    logger.warning("伤害校验后台暂缓: {}", type(exc).__name__)
                    self._next_error_log = self.clock() + 60
            await asyncio.sleep(self.GLOBAL_INTERVAL)


damage_reconciler = ReconciliationCoordinator(ReconciliationStore(pcr_sqla.engine))
