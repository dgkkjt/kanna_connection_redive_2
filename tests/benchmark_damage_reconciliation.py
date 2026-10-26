"""Offline foreground microbenchmark; no game traffic or production writes.

Includes real Pydantic parsing of 100 reports with five units each. Reports
CPU and P95 local processing cost, not production network/SQLite contention.
"""

import asyncio
import gc
import json
import statistics
import time
import tracemalloc

from test_damage_reconciliation import S, history, reconciliation, tracker_module
from damage_test_app.client.response import ClanBattleLogListResponse


class Monitor:
    def __init__(self, index):
        self.group_id = 1000 + index
        self.clan_id = 2000 + index
        self.clan_battle_id = 99
        self.client = S(platfrom=0, viewer_id=index)
        self.battle_period = 1
        self.loop_num = 1


DATA = dict(max_page=1, battle_list=[dict(target_viewer_id=i + 1,
    battle_log_id=i + 1, lap_num=27, order_num=5, battle_end_time=1000 + i,
    total_damage=1000, units=[dict(unit_id=100101 + j * 100, damage=200,
        unit_level=100, unit_rarity=5, promotion_level=30) for j in range(5)])
    for i in range(100)])
TOP = S(boss_info=[S(lap_num=27, order_num=i, max_hp=1480000000, current_hp=1000000000)
                  for i in range(1, 6)], damage_history=[history(1, 1000, 1000)])


async def sample(count, rounds, enabled):
    virtual_now = time.time()
    worker = reconciliation.ReconciliationCoordinator(None, clock=lambda: virtual_now)
    clans = [Monitor(i) for i in range(count)]
    trackers = [tracker_module.DamageCorrectionTracker() for _ in clans]
    for clan, tracker in zip(clans, trackers):
        tracker.reset(TOP)
        if enabled:
            await worker.register(clan, TOP)
    latencies = []
    gc.collect()
    start = time.process_time()
    for _ in range(rounds):
        virtual_now += 15  # normal monitor interval, independent of benchmark speed
        for clan, tracker in zip(clans, trackers):
            started = time.perf_counter()
            response = ClanBattleLogListResponse.parse_obj(DATA)
            histories = tracker.update(TOP)
            if enabled:
                await worker.observe(clan, TOP, histories)
                worker.offer_page(clan, response)
            latencies.append(time.perf_counter() - started)
    return dict(cpu=time.process_time() - start,
                p95=sorted(latencies)[int(len(latencies) * .95) - 1])


async def main():
    results = []
    await sample(1, 10, True)  # warm imports and allocators before comparison
    for count, rounds in ((1, 100), (10, 20), (50, 8)):
        baseline, enabled = [], []
        for trial in range(5):
            order = (False, True) if trial % 2 == 0 else (True, False)
            for mode in order:
                result = await sample(count, rounds, mode)
                (enabled if mode else baseline).append(result)
        cpu0 = statistics.median(r["cpu"] for r in baseline)
        cpu1 = statistics.median(r["cpu"] for r in enabled)
        p950 = statistics.median(r["p95"] for r in baseline)
        p951 = statistics.median(r["p95"] for r in enabled)
        results.append(dict(groups=count, reports_per_poll=100,
            baseline_cpu_seconds=round(cpu0, 4), enabled_cpu_seconds=round(cpu1, 4),
            cpu_change_percent=round((cpu1 / cpu0 - 1) * 100, 2),
            baseline_processing_p95_ms=round(p950 * 1000, 3),
            enabled_processing_p95_ms=round(p951 * 1000, 3)))
    tracemalloc.start()
    worker = reconciliation.ReconciliationCoordinator(None)
    clans = [Monitor(i) for i in range(50)]
    for clan in clans:
        await worker.register(clan, TOP)
        response = ClanBattleLogListResponse.parse_obj(DATA)
        for i in range(4):
            response.battle_list[0].battle_log_id = i + 1
            worker.offer_page(clan, response)
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(json.dumps(dict(foreground=results, memory=dict(
        traced_current_bytes=current, traced_peak_bytes=peak,
        cached_pages=len(worker._pages), cache_payload_bytes=worker._cache_bytes,
        pending_observations=len(worker._observations)),
        limits="Offline foreground microbenchmark; real network and production contention excluded."),
        indent=2))


if __name__ == "__main__":
    asyncio.run(main())
