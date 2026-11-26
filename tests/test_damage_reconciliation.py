"""Run with the bot's Python: python -m unittest discover -s tests -v.

Load the real storage/client modules without importing Hoshino services or
starting a bot. All database writes use temporary files, never data.db.
"""

import asyncio
import importlib
import json
import pathlib
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import AsyncMock, patch

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool
from loguru import logger


ROOT = pathlib.Path(__file__).resolve().parents[1]
PACKAGE = "damage_test_app"
logger.disable(PACKAGE)
for suffix in ("", ".client", ".database", ".clanbattle", ".util"):
    name = PACKAGE + suffix
    module = types.ModuleType(name)
    module.__path__ = [str(ROOT.joinpath(*suffix.strip(".").split(".")))] if suffix else [str(ROOT)]
    sys.modules[name] = module
auto_boss = types.ModuleType(PACKAGE + ".util.auto_boss")
auto_boss.clan_boss_info = types.SimpleNamespace(get_boss_max=lambda lap, boss: 1480000000)
sys.modules[auto_boss.__name__] = auto_boss

storage = importlib.import_module(PACKAGE + ".database.reconciliation")
tracker_module = importlib.import_module(PACKAGE + ".clanbattle.damage_correction")
base = importlib.import_module(PACKAGE + ".client.base")
models = importlib.import_module(PACKAGE + ".database.models")
dal = importlib.import_module(PACKAGE + ".database.dal")
reconciliation = importlib.import_module(PACKAGE + ".clanbattle.reconciliation")
S = types.SimpleNamespace


def report(viewer, damage, timestamp, lap=27, boss=5, report_id=None):
    return S(target_viewer_id=viewer, total_damage=damage, battle_end_time=timestamp,
             battle_log_id=report_id or viewer, lap_num=lap, order_num=boss)


def history(viewer, damage, timestamp, kill=0):
    return S(viewer_id=viewer, damage=damage, create_time=timestamp, kill=kill,
             lap_num=27, order_num=5, history_id=viewer, name=str(viewer))


INCIDENT = [report(1281735851555, 299763866, 1792988542),
            report(1051975621196, 306891228, 1792990280),
            report(1213403896349, 352315787, 1792993661),
            report(1303739563598, 335456511, 1793008587),
            report(1343423659518, 334980665, 1793008591)]


class StoreFixture(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.engine = create_async_engine("sqlite+aiosqlite:///" + self.temp.name + "/test.db", poolclass=NullPool)
        async with self.engine.begin() as conn:
            await conn.execute(text("""CREATE TABLE recorddao (
                id INTEGER PRIMARY KEY,group_id BIGINT,lap INTEGER,boss INTEGER,
                pcrid BIGINT,time BIGINT,damage BIGINT)"""))
            await storage.migrate(conn)
        self.store = storage.ReconciliationStore(self.engine)
        self.now = time.time()
        await self.store.enqueue("cn:0", 123, 99, 1, 27, 5, 619935706, 1480000000,
                                 (1343423659518, 1793008591))
        self.job = await self.store.due(self.now + 1)

    async def asyncTearDown(self):
        await self.store.engine.dispose()
        await self.engine.dispose()
        self.temp.cleanup()

    async def fetch(self, sql, **args):
        async with self.engine.connect() as conn:
            return [dict(r) for r in (await conn.execute(text(sql), args)).mappings()]

    async def seed_rows(self, group=619935706, battle=None):
        async with self.engine.begin() as conn:
            for r in INCIDENT:
                await conn.execute(text("""INSERT INTO recorddao
                    (group_id,lap,boss,pcrid,time,damage,clan_battle_id)
                    VALUES (:group,27,5,:viewer,:end,:damage,:battle)"""),
                    dict(group=group, viewer=r.target_viewer_id, end=r.battle_end_time,
                         damage=r.total_damage, battle=battle))

    async def complete(self):
        await self.store.add_page(self.job, INCIDENT)
        self.job.update(expected=5, clear_time=1793008591)
        result = await self.store.evaluate(self.job, [])
        await self.store.update(self.job["job_key"], status="complete", result=storage.encode(result), due=self.now)
        return result


class StoreTests(StoreFixture):
    async def test_migration_idempotent_preserves_unknown_raw(self):
        await self.seed_rows()
        async with self.engine.begin() as conn:
            await storage.migrate(conn)
            await storage.migrate(conn)
        rows = await self.fetch("SELECT raw_damage,damage FROM recorddao")
        self.assertTrue(all(r["raw_damage"] is None for r in rows))
        self.assertEqual([r["damage"] for r in rows], [r.total_damage for r in INCIDENT])

    async def test_incident_atomic_apply_and_group_isolation(self):
        await self.seed_rows()
        await self.seed_rows(group=999)
        result = await self.complete()
        self.assertEqual(result["damage"], 185572608)
        await self.store.apply_one(self.now + 1)  # raw backfill
        applied = await self.store.apply_one(self.now + 2)
        self.assertEqual(applied["applied"], 1)
        self.assertIsNone(await self.store.apply_one(self.now + 3))
        rows = await self.fetch("SELECT * FROM recorddao WHERE group_id=619935706 ORDER BY time")
        self.assertEqual(rows[-2]["damage"], 335456511)
        self.assertEqual(rows[-1]["damage"], 185572608)
        self.assertEqual(rows[-1]["raw_damage"], 334980665)
        self.assertEqual(rows[-1]["correction_method"], "boss_total")
        self.assertEqual(json.loads(rows[-1]["correction_evidence"])["attack_count"], 5)
        other = await self.fetch("SELECT damage,raw_damage FROM recorddao WHERE group_id=999 ORDER BY time")
        self.assertEqual(other[-1]["damage"], 334980665)
        self.assertIsNone(other[-1]["raw_damage"])

    async def test_shared_job_and_new_group_subscription(self):
        await self.seed_rows()
        await self.complete()
        await self.store.enqueue("cn:0", 123, 99, 1, 27, 5, 777, 1480000000)
        self.assertEqual(len(await self.fetch("SELECT * FROM damage_recon_job")), 1)
        await self.seed_rows(group=777)
        for i in range(5):
            await self.store.apply_one(self.now + 1 + i)
        rows = await self.fetch("SELECT damage FROM recorddao WHERE group_id=777 ORDER BY time")
        self.assertEqual(rows[-1]["damage"], 185572608)

    async def test_missing_detail_then_restart_recovers(self):
        await self.complete()
        self.assertEqual((await self.store.apply_one(self.now + 1))["applied"], 0)
        self.store = storage.ReconciliationStore(self.engine)
        await self.seed_rows()
        await self.store.apply_one(self.now + 32)
        self.assertEqual((await self.store.apply_one(self.now + 33))["applied"], 1)

    async def test_conflicting_previous_damage_does_not_change_tail(self):
        await self.seed_rows()
        async with self.engine.begin() as conn:
            await conn.execute(text("UPDATE recorddao SET damage=1 WHERE pcrid=1281735851555"))
        await self.complete()
        self.assertEqual((await self.store.apply_one(self.now + 1))["applied"], -1)
        tail = await self.fetch("SELECT damage FROM recorddao WHERE pcrid=1343423659518")
        self.assertEqual(tail[0]["damage"], 334980665)

    async def test_conflicting_snapshot_is_preserved(self):
        await self.seed_rows()
        async with self.engine.begin() as conn:
            await conn.execute(text("UPDATE recorddao SET damage=7,correction_method='snapshot' WHERE pcrid=1343423659518"))
        await self.complete()
        await self.store.apply_one(self.now + 1)
        self.assertEqual((await self.store.apply_one(self.now + 2))["applied"], -1)
        self.assertEqual((await self.fetch("SELECT damage FROM recorddao WHERE pcrid=1343423659518"))[0]["damage"], 7)

    async def test_missing_reports_count_is_not_complete(self):
        await self.store.add_page(self.job, INCIDENT[:-1])
        self.job.update(expected=5, clear_time=1793008591)
        with self.assertRaisesRegex(ValueError, "attack_count_mismatch"):
            await self.store.evaluate(self.job, [])

    async def test_duplicate_poll_does_not_increase_count(self):
        self.assertEqual(await self.store.add_page(self.job, INCIDENT), 5)
        self.assertEqual(await self.store.add_page(self.job, INCIDENT), 5)
        changed = report(INCIDENT[0].target_viewer_id, 1, INCIDENT[0].battle_end_time)
        with self.assertRaisesRegex(ValueError, "report_changed"):
            await self.store.add_page(self.job, [changed])

    async def test_ambiguous_tail_second_requires_kill_evidence(self):
        self.job["kill_key"] = None
        same_second = [report(1, 900, 100), report(2, 900, 100)]
        await self.store.add_page(self.job, same_second)
        self.job.update(expected=2, clear_time=100, max_hp=1500)
        with self.assertRaisesRegex(ValueError, "ambiguous_kill"):
            await self.store.evaluate(self.job, [])
        result = await self.store.evaluate(self.job, [history(2, 900, 100, 1)])
        self.assertEqual(result["damage"], 600)

    async def test_duplicate_player_second_is_ambiguous(self):
        a = report(2, 900, 100, report_id=1)
        b = report(2, 900, 100, report_id=2)
        await self.store.add_page(self.job, [a, b])
        self.job.update(expected=2, clear_time=100, max_hp=1500)
        with self.assertRaisesRegex(ValueError, "ambiguous_report_key"):
            await self.store.evaluate(self.job, [])

    async def test_hp_contradiction_refuses_correction(self):
        await self.store.add_page(self.job, INCIDENT)
        self.job.update(expected=5, clear_time=1793008591, max_hp=100)
        with self.assertRaisesRegex(ValueError, "hp_contradiction"):
            await self.store.evaluate(self.job, [])

    async def test_server_hp_conflict_stops_task(self):
        await self.store.save_bosses("cn:0", 123, 99, [S(lap_num=27, order_num=5, max_hp=1)])
        row = (await self.fetch("SELECT status,reason FROM damage_recon_job"))[0]
        self.assertEqual(row, dict(status="unresolved", reason="hp_source_conflict"))

    async def test_cross_battle_does_not_match(self):
        await self.seed_rows(battle=88)
        await self.complete()
        self.assertEqual((await self.store.apply_one(self.now + 1))["applied"], 0)
        rows = await self.fetch("SELECT damage FROM recorddao ORDER BY time")
        self.assertEqual(rows[-1]["damage"], 334980665)

    async def test_write_failure_rolls_back_correction(self):
        await self.seed_rows()
        await self.complete()
        await self.store.apply_one(self.now + 1)
        async with self.engine.begin() as conn:
            await conn.execute(text("""CREATE TRIGGER fail_apply BEFORE UPDATE OF damage ON recorddao
                BEGIN SELECT RAISE(ABORT,'test failure'); END"""))
        with self.assertRaises(Exception):
            await self.store.apply_one(self.now + 2)
        rows = await self.fetch("SELECT damage,correction_method FROM recorddao ORDER BY time")
        self.assertEqual(rows[-1]["damage"], 334980665)
        self.assertEqual(rows[-1]["correction_method"], "")
        self.assertEqual((await self.fetch("SELECT applied FROM damage_recon_group"))[0]["applied"], 0)

    async def test_reset_deletes_at_most_one_hundred_reports(self):
        records = [report(i + 1, 1, i + 100) for i in range(201)]
        for start in range(0, len(records), 100):
            await self.store.add_page(self.job, records[start:start + 100])
        await self.store.reset(self.job["job_key"], due=self.now)
        self.assertEqual((await self.fetch("SELECT count(*) AS n FROM damage_recon_report"))[0]["n"], 101)
        self.assertEqual((await self.store.due(self.now))["stage"], "reset")
        await self.store.reset(self.job["job_key"], due=self.now)
        await self.store.reset(self.job["job_key"], due=self.now)
        self.assertEqual((await self.store.due(self.now))["stage"], "before")


class TrackerTests(unittest.TestCase):
    def test_simultaneous_and_separate_reports_have_same_cap(self):
        old = history(1, 352315787, 1792993661)
        a = history(1303739563598, 335456511, 1793008587)
        b = history(1343423659518, 334980665, 1793008591, 1)
        def top(hs, hp):
            return S(damage_history=hs, boss_info=[S(lap_num=27, order_num=5, current_hp=hp)])
        for separate in (False, True):
            tracker = tracker_module.DamageCorrectionTracker()
            tracker.reset(top([old], 521029119))
            if separate:
                tracker.update(top([a, old], 185572608))
            tracker.update(top([b, a, old], 0))
            self.assertEqual(tracker.history_damage(b), 185572608)
            self.assertEqual(tracker.history_damage(a), 335456511)
            self.assertEqual(tracker.evidence[(b.viewer_id, 27, 5, b.create_time)]["remaining_hp"], 185572608)


class LockTests(unittest.IsolatedAsyncioTestCase):
    async def test_background_never_waits_for_foreground(self):
        lock = base.CrossLoopLock()
        async with lock:
            token = base.background_request.set(True)
            try:
                with self.assertRaises(base.BackgroundBusy):
                    async with lock:
                        self.fail("background entered foreground lock")
            finally:
                base.background_request.reset(token)

    async def test_timeout_releases_account_lock(self):
        class Client(base.BaseClient):
            async def callapi(self, request):
                async with self._api_lock:
                    await asyncio.sleep(60)
        client = Client()
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(client.reconciliation_call(None), .01)
        async with client._api_lock:
            pass

    async def test_foreground_waiter_has_priority(self):
        lock = base.CrossLoopLock()
        await lock.__aenter__()
        entered = []
        async def foreground():
            async with lock:
                entered.append(True)
        task = asyncio.create_task(foreground())
        await asyncio.sleep(0)
        await lock.__aexit__(None, None, None)
        token = base.background_request.set(True)
        try:
            with self.assertRaises(base.BackgroundBusy):
                async with lock:
                    self.fail("background overtook waiter")
        finally:
            base.background_request.reset(token)
        await task
        self.assertEqual(entered, [True])


class SchedulerTests(StoreFixture):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.fake_now = self.now + 1
        self.worker = reconciliation.ReconciliationCoordinator(self.store, clock=lambda: self.fake_now)
        self.calls = []
        owner = self
        class Client:
            viewer_id = 42
            platfrom = 0
            async def reconciliation_call(self, request):
                owner.calls.append((owner.fake_now, request.url))
                if request.url.endswith("boss_info"):
                    return dict(current_hp=0, attack_count=5, damage_history=[])
                if request.url.endswith("boss_history"):
                    return dict(boss_history=[dict(lap_num=27, order_num=5,
                        attack_count=5, clear_time=1793008591)])
                return dict(max_page=1, battle_list=[dict(vars(r), units=[]) for r in INCIDENT])
        self.client = Client()
        self.clans = []
        class Monitor:
            def __init__(self, **values):
                self.__dict__.update(values)
        for i in range(50):
            clan = Monitor(group_id=1000 + i, clan_id=123, clan_battle_id=99, client=self.client,
                     loop_check=1, loop_num=1, _recon_loop_num=1, dao_update_time=0)
            self.clans.append(clan)
            self.worker._groups[clan.group_id] = clan

    async def test_fifty_monitors_share_one_acquisition_and_rate_limit(self):
        for clan in self.clans:
            await self.store.enqueue("cn:0", 123, 99, 1, 27, 5, clan.group_id, 1480000000)
        for i in range(24):
            await self.worker.tick()
            self.fake_now += 5
        rows = await self.fetch("SELECT status FROM damage_recon_job")
        self.assertEqual(rows, [dict(status="complete")])
        self.assertEqual(len(self.calls), 4)
        self.assertTrue(all(b[0] - a[0] >= 30 for a, b in zip(self.calls, self.calls[1:])))

    async def test_reuses_normal_monitor_page(self):
        self.worker.offer_page(self.clans[0], S(battle_list=INCIDENT))
        for i in range(20):
            # Refresh the normal monitor's cache as it would during polling.
            if i % 3 == 0:
                self.worker.offer_page(self.clans[0], S(battle_list=INCIDENT))
            await self.worker.tick()
            self.fake_now += 5
        self.assertFalse(any(url.endswith("battle_log_list") for _, url in self.calls))
        self.assertEqual(len(self.calls), 3)

    async def test_cancelled_monitor_does_not_use_account(self):
        for clan in self.clans:
            clan.loop_num += 1
        await self.worker.tick()
        self.assertEqual(self.calls, [])

    async def test_cache_and_task_memory_are_bounded(self):
        for i in range(1000):
            self.worker.offer_page(self.clans[0], S(battle_list=[report(i + 1, 1, i)]))
        self.assertLessEqual(len(self.worker._pages), 128)
        self.assertLessEqual(self.worker._cache_bytes, 8 * 1024 * 1024)
        self.assertEqual(len(self.worker._groups), 50)
        self.assertIsNone(self.worker._task)

    async def test_foreground_submissions_do_not_access_database(self):
        store = AsyncMock()
        worker = reconciliation.ReconciliationCoordinator(store)
        top = S(boss_info=[S(lap_num=27, order_num=i, max_hp=1480000000) for i in range(1, 6)])
        for clan in self.clans:
            await worker.register(clan, top)
            await worker.observe(clan, top, [history(2, 100, 100, 1)])
        store.save_bosses.assert_not_awaited()
        store.enqueue.assert_not_awaited()
        store.subscribe.assert_not_awaited()
        self.assertEqual(len(worker._observations), 5)
        await worker.flush_observations()
        self.assertEqual(store.enqueue.await_count, 1)
        self.assertEqual(len(store.enqueue.await_args.args[6]), 50)

    async def test_busy_database_retains_submission_and_monitor_continues(self):
        store = AsyncMock()
        store.save_bosses.side_effect = RuntimeError("busy")
        worker = reconciliation.ReconciliationCoordinator(store)
        clan = self.clans[0]
        await worker.register(clan, S(boss_info=[S(lap_num=27, order_num=5, max_hp=1480000000)]))
        with self.assertRaises(RuntimeError):
            await worker.flush_observations()
        self.assertEqual(len(worker._observations), 1)
        store.save_bosses.side_effect = None
        await worker.flush_observations()
        self.assertEqual(len(worker._observations), 0)

    async def test_expiration_is_persistent(self):
        self.fake_now += 86401
        await self.worker.tick()
        rows = await self.fetch("SELECT status FROM damage_recon_job")
        self.assertEqual(rows, [dict(status="unresolved")])

    async def test_changed_server_count_does_not_correct(self):
        await self.store.add_page(self.job, INCIDENT)
        self.job.update(stage="after", expected=6, clear_time=1793008591)
        with self.assertRaisesRegex(ValueError, "attack_count_changed"):
            await self.worker.step(self.job, self.client)

    async def test_empty_worker_checks_database_only_every_thirty_seconds(self):
        store = AsyncMock()
        store.apply_one.return_value = None
        store.due.return_value = None
        worker = reconciliation.ReconciliationCoordinator(store, clock=lambda: self.fake_now)
        for _ in range(12):
            await worker.tick()
            self.fake_now += 5
        self.assertEqual(store.due.await_count, 2)

    async def test_real_sqlite_writer_contention_times_out_quickly(self):
        clan = self.clans[0]
        await self.worker.register(clan, S(boss_info=[S(lap_num=27, order_num=5, max_hp=1480000000)]))
        async with self.engine.begin() as conn:
            await conn.execute(text("UPDATE damage_recon_job SET reason='lock held by foreground'"))
            started = time.perf_counter()
            with self.assertRaises(Exception):
                await self.worker.flush_observations()
            self.assertLess(time.perf_counter() - started, .5)
        self.assertTrue(self.worker._observations)
        await self.worker.flush_observations()
        self.assertFalse(self.worker._observations)

    async def test_different_clans_share_global_budget_without_mixing_results(self):
        # Each QQ group represents a different clan and account, in one process.
        events = []
        for i, clan in enumerate(self.clans):
            clan.clan_id = 123 + i
            client = type(self.client)()
            client.viewer_id = 42 + i
            original = client.reconciliation_call
            async def call(request, original=original, account=client.viewer_id):
                events.append((self.fake_now, account, request.clan_id if hasattr(request, 'clan_id') else None))
                return await original(request)
            client.reconciliation_call = call
            clan.client = client
            await self.store.enqueue("cn:0", clan.clan_id, 99, 1, 27, 5, clan.group_id,
                                     1480000000, (1343423659518, 1793008591))
        for i in range(240):
            await self.worker.tick()
            self.fake_now += 5
        jobs = await self.fetch("SELECT clan_id,status,result FROM damage_recon_job")
        self.assertEqual(len(jobs), 50)
        self.assertTrue(all(j["status"] == "complete" for j in jobs))
        self.assertEqual(len(events), 200)
        self.assertTrue(all(b[0] - a[0] >= 5 for a, b in zip(events, events[1:])))
        for account in range(42, 92):
            own = [e for e in events if e[1] == account]
            self.assertEqual(len(own), 4)
            self.assertTrue(all(b[0] - a[0] >= 30 for a, b in zip(own, own[1:])))
        for job in jobs:
            self.assertTrue(json.loads(job["result"])["job_key"].startswith("cn:0:%s:" % job["clan_id"]))


class MonitorIntegrationTests(unittest.IsolatedAsyncioTestCase):
    """Import the actual monitor with only messaging and Hoshino glue stubbed."""
    @classmethod
    def setUpClass(cls):
        client_package = sys.modules[PACKAGE + ".client"]
        client_package.BaseClient = base.BaseClient
        client_package.LoadIndexResponse = importlib.import_module(PACKAGE + ".client.response").LoadIndexResponse
        client_package.check_client = AsyncMock()
        login = types.ModuleType(PACKAGE + ".login")
        login.run_group = {}
        sys.modules[login.__name__] = login
        tools = types.ModuleType(PACKAGE + ".util.tools")
        tools.anywhere_send = AsyncMock()
        sys.modules[tools.__name__] = tools
        battle_base = types.ModuleType(PACKAGE + ".clanbattle.base")
        for name in ("find_item", "format_bignum", "format_precent"):
            setattr(battle_base, name, lambda *args: "")
        sys.modules[battle_base.__name__] = battle_base
        cls.monitor_module = importlib.import_module(PACKAGE + ".clanbattle.model")

    async def test_monitor_saves_raw_and_effective_damage_and_snapshot_evidence(self):
        clan = self.monitor_module.ClanBattle(619935706)
        clan.clan_battle_id = 99
        clan.client = S(time_line_report=AsyncMock(return_value=S(start_remain_time=90, battle_time=90)))
        old = history(1, 352315787, 1792993661)
        a = history(1303739563598, 335456511, 1793008587)
        b = history(1343423659518, 334980665, 1793008591, 1)
        def top(hs, hp):
            return S(damage_history=hs, boss_info=[S(lap_num=27, order_num=5, current_hp=hp)])
        clan.damage_correction.reset(top([old], 521029119))
        clan.notice_text = AsyncMock(return_value="")
        record = S(**vars(INCIDENT[-1]), user_name="小心爱", units=[])
        page = S(max_page=1, battle_list=[record])
        clan.get_battle_log = AsyncMock(return_value=page)
        db = S(get_latest_time=AsyncMock(return_value=0), get_record_keys_at=AsyncMock(return_value=set()),
               add_record=AsyncMock(), save_battle_entries=AsyncMock())
        worker = S(observe=AsyncMock())
        with patch.object(self.monitor_module, "damage_reconciler", worker), patch.object(self.monitor_module, "pcr_sqla", db):
            self.assertTrue(await clan.record_change(top([b, a, old], 0)))
        self.assertIn("185,572,608", clan.notice_dao[0])
        self.assertIn("预计返秒61s", clan.notice_dao[0])
        db.add_record.assert_awaited_once()
        saved, = db.add_record.await_args.args[0]
        self.assertEqual(saved.damage, 185572608)
        self.assertEqual(saved.raw_damage, 334980665)
        self.assertEqual(saved.clan_battle_id, 99)
        self.assertEqual(saved.server_battle_log_id, record.battle_log_id)
        self.assertEqual(saved.correction_method, "snapshot")
        self.assertEqual(json.loads(saved.correction_evidence)["remaining_hp"], 185572608)

    async def test_timer_save_retries_without_interrupting_reports(self):
        clan = self.monitor_module.ClanBattle(1)
        clan.entries.observe(1, 2, 1, 100)
        clan.add_record = AsyncMock(return_value={})
        top = S(damage_history=[], boss_info=[])
        db = S(save_battle_entries=AsyncMock(side_effect=[OSError(), None]))
        worker = S(observe=AsyncMock())
        with patch.object(self.monitor_module, "pcr_sqla", db), patch.object(self.monitor_module, "damage_reconciler", worker):
            for _ in range(3):
                self.assertFalse(await clan.record_change(top))
        self.assertEqual(db.save_battle_entries.await_count, 2)
        self.assertEqual(worker.observe.await_count, 3)
        clan.add_record.assert_not_awaited()
        restored = self.monitor_module.EntryTracker("", clan.saved_entry_state)
        self.assertEqual(restored.active_bosses(), [1])
        restored.settle(S(history_id=1, viewer_id=1, order_num=1, lap_num=2, create_time=101, kill=1))
        self.assertEqual(restored.active_bosses(), [])

    async def test_poll_fetches_details_once_with_and_without_kills(self):
        module = self.monitor_module
        for has_kill in (False, True):
            with self.subTest(has_kill=has_kill):
                clan = module.ClanBattle(1)
                clan.loop_num = clan.bot_id = 1
                clan.get_clanbattle_top = AsyncMock(return_value=S(
                    lap_num=0, period_rank=1, damage_history=[], boss_info=[]))
                clan.refresh_boss = AsyncMock()
                clan.send_notice = AsyncMock()
                clan.notice_text = AsyncMock(return_value="")
                clan.add_record = AsyncMock(return_value={})
                clan.reconcile_damage = AsyncMock()
                histories = [history(1, 200, 10, kill=1)] if has_kill else []
                clan.damage_correction.update = lambda _: histories
                clan.damage_correction.cap_for = lambda _: 100
                pool = module.ClanBattlePool(1)
                pool.add_task = AsyncMock()
                item = module.PrioritizedQueryItem(data=module.ClanbattleItem(clan, 1))
                with patch.object(module, "damage_reconciler", S(observe=AsyncMock())):
                    await pool.do_single(item)
                    await asyncio.sleep(0)
                clan.add_record.assert_awaited_once_with(1)
                clan.reconcile_damage.assert_awaited_once()
                pool.add_task.assert_awaited_once()

    async def test_delayed_detail_reconciliation_passes_battle_and_evidence(self):
        clan = self.monitor_module.ClanBattle(619935706)
        clan.clan_battle_id = 99
        key = (1, 27, 5, 100)
        clan.damage_correction.pending_caps[key] = 50
        clan.damage_correction.evidence[key] = {"remaining_hp": 50}
        db = S(correct_kill_damage=AsyncMock())
        with patch.object(self.monitor_module, "pcr_sqla", db):
            await clan.reconcile_damage()
        db.correct_kill_damage.assert_awaited_once_with(619935706, {key: 50}, {key: {"remaining_hp": 50}}, 99)
        self.assertEqual(clan.damage_correction.pending_caps, {})


class ORMIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_sl_and_timer_commit_once_and_rollback_together(self):
        with tempfile.TemporaryDirectory() as directory:
            db = dal.SQALA(directory + "/sl.db")
            await db.create_all()
            await db.save_battle_entries(1, "before")
            async with db.engine.begin() as conn:
                await conn.execute(text("""CREATE TRIGGER fail_timer BEFORE UPDATE ON battleentrystate
                    BEGIN SELECT RAISE(ABORT, 'test timer failure'); END"""))
            sl = models.SLDao(group_id=1, user_id=2, time=int(time.time()))
            with self.assertRaises(IntegrityError):
                await db.add_sl(sl, "after")
            self.assertFalse(await db.check_sl(2, 1))
            self.assertEqual(await db.get_battle_entries(1), "before")
            async with db.engine.begin() as conn:
                await conn.execute(text("DROP TRIGGER fail_timer"))
            accepted = await asyncio.gather(db.add_sl(sl, "first"), db.add_sl(sl, "second"))
            self.assertEqual(sum(accepted), 1)
            self.assertEqual(await db.get_battle_entries(1), "first" if accepted[0] else "second")
            self.assertTrue(await db.check_sl(2, 1))
            async with db.engine.begin() as conn:
                await conn.execute(text("UPDATE sldao SET time=NULL"))
            self.assertTrue(await db.add_sl(sl))
            await db.engine.dispose()

    async def test_real_schema_and_delayed_snapshot_correction(self):
        with tempfile.TemporaryDirectory() as directory:
            db = dal.SQALA(directory + "/orm.db")
            await db.create_all()
            await db.create_all()
            row = models.RecordDao(group_id=1, battle_log_id=100, lap=27, boss=5,
                time=100, pcrid=2, damage=300, raw_damage=300, clan_battle_id=99,
                server_battle_log_id=3, name="test", remain_time=90, battle_time=90, flag=0,
                unit1=100101, unit1_level=1, unit1_damage=300, unit1_rarity=1,
                unit1_rank=1, unit1_unique_equip=0)
            await db.add_record([row])
            key = (2, 27, 5, 100)
            await db.correct_kill_damage(1, {key: 100}, {key: {"remaining_hp": 100}}, 88)
            await db.correct_kill_damage(1, {key: 100}, {key: {"remaining_hp": 100}}, 99)
            async with db.async_session() as session:
                values = (await session.execute(text("SELECT damage,raw_damage,correction_method FROM recorddao"))).one()
            self.assertEqual(tuple(values), (100, 300, "snapshot"))
            await db.engine.dispose()


if __name__ == "__main__":
    unittest.main()
