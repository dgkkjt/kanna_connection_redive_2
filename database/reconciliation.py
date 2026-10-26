"""Bounded, persistent evidence for clan-battle damage reconciliation.

Uses the application's engine, but never holds a transaction across game I/O.
The server's per-boss attack count is required; a finished page scan alone is
not evidence that the game retained every report.
"""

import json
import time

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool


RECORD_COLUMNS = {
    "raw_damage": "BIGINT",
    "clan_battle_id": "BIGINT",
    "server_battle_log_id": "BIGINT",
    "correction_method": "VARCHAR NOT NULL DEFAULT ''",
    "correction_evidence": "TEXT NOT NULL DEFAULT '{}'",
    "corrected_at": "BIGINT NOT NULL DEFAULT 0",
}


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


async def migrate(conn):
    columns = {r[1] for r in (await conn.execute(text("PRAGMA table_info(recorddao)")))}
    for name, definition in RECORD_COLUMNS.items():
        if name not in columns:
            await conn.execute(text("ALTER TABLE recorddao ADD COLUMN %s %s" % (name, definition)))
    for statement in (
        "CREATE INDEX IF NOT EXISTS ix_record_recon ON recorddao(group_id,clan_battle_id,lap,boss,pcrid,time)",
        "CREATE INDEX IF NOT EXISTS ix_record_recon_legacy ON recorddao(group_id,lap,boss,pcrid,time)",
        """CREATE TABLE IF NOT EXISTS damage_recon_job (
            job_key TEXT PRIMARY KEY, realm TEXT NOT NULL, clan_id BIGINT NOT NULL,
            battle_id BIGINT NOT NULL, period INTEGER NOT NULL, lap INTEGER NOT NULL,
            boss INTEGER NOT NULL, max_hp BIGINT NOT NULL, hp_source TEXT NOT NULL,
            created BIGINT NOT NULL, updated BIGINT NOT NULL, due REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', stage TEXT NOT NULL DEFAULT 'before',
            page INTEGER NOT NULL DEFAULT 1, cached_seq BIGINT NOT NULL DEFAULT 0,
            attempts INTEGER NOT NULL DEFAULT 0,
            expected INTEGER, clear_time BIGINT, kill_key TEXT,
            reason TEXT NOT NULL DEFAULT '', result TEXT NOT NULL DEFAULT '{}')""",
        "CREATE INDEX IF NOT EXISTS ix_recon_due ON damage_recon_job(status,due)",
        "CREATE INDEX IF NOT EXISTS ix_recon_clan ON damage_recon_job(realm,clan_id,battle_id)",
        "CREATE INDEX IF NOT EXISTS ix_recon_created ON damage_recon_job(created)",
        """CREATE TABLE IF NOT EXISTS damage_recon_group (
            job_key TEXT NOT NULL, group_id BIGINT NOT NULL, applied INTEGER NOT NULL DEFAULT 0,
            last_report BIGINT NOT NULL DEFAULT 0, reason TEXT NOT NULL DEFAULT '',
            PRIMARY KEY(job_key,group_id))""",
        "CREATE INDEX IF NOT EXISTS ix_recon_unapplied ON damage_recon_group(applied,job_key)",
        """CREATE TABLE IF NOT EXISTS damage_recon_report (
            job_key TEXT NOT NULL, viewer_id BIGINT NOT NULL, report_id BIGINT NOT NULL,
            end_time BIGINT NOT NULL, damage BIGINT NOT NULL,
            PRIMARY KEY(job_key,viewer_id,report_id))""",
        """CREATE TABLE IF NOT EXISTS damage_recon_boss (
            realm TEXT NOT NULL, clan_id BIGINT NOT NULL, battle_id BIGINT NOT NULL,
            lap INTEGER NOT NULL, boss INTEGER NOT NULL, max_hp BIGINT NOT NULL,
            updated BIGINT NOT NULL, PRIMARY KEY(realm,clan_id,battle_id,lap,boss))""",
    ):
        await conn.execute(text(statement))


class ReconciliationStore:
    def __init__(self, engine):
        # Background writes give up quickly rather than queuing behind foreground
        # SQLite writers. NullPool retains no idle connections or worker threads.
        self.engine = create_async_engine(engine.url, poolclass=NullPool,
                                         connect_args={"timeout": .1})

    async def save_bosses(self, realm, clan_id, battle_id, bosses):
        values = [dict(realm=realm, clan=clan_id, battle=battle_id, lap=b.lap_num,
                       boss=b.order_num, hp=b.max_hp, now=int(time.time()))
                  for b in bosses or [] if isinstance(b.max_hp, int) and b.max_hp > 0]
        if not values:
            return
        async with self.engine.begin() as conn:
            await conn.execute(text("""INSERT INTO damage_recon_boss
                VALUES (:realm,:clan,:battle,:lap,:boss,:hp,:now)
                ON CONFLICT(realm,clan_id,battle_id,lap,boss) DO UPDATE SET
                updated=excluded.updated"""), values)
            await conn.execute(text("""UPDATE damage_recon_job SET status='unresolved',
                reason='hp_source_conflict' WHERE realm=:realm AND clan_id=:clan AND battle_id=:battle
                AND lap=:lap AND boss=:boss AND max_hp<>:hp AND status IN ('pending','complete')"""), values)

    async def subscribe(self, realm, clan_id, battle_id, group_id):
        async with self.engine.begin() as conn:
            await conn.execute(text("""INSERT OR IGNORE INTO damage_recon_group(job_key,group_id)
                SELECT job_key,:group FROM damage_recon_job WHERE realm=:realm AND clan_id=:clan
                AND battle_id=:battle AND created>=:cutoff"""), dict(group=group_id, realm=realm,
                clan=clan_id, battle=battle_id, cutoff=int(time.time()) - 86400))

    async def enqueue(self, realm, clan_id, battle_id, period, lap, boss,
                      group_id, config_hp, kill_key=None):
        now = int(time.time())
        key = "%s:%s:%s:%s:%s" % (realm, clan_id, battle_id, lap, boss)
        async with self.engine.begin() as conn:
            hp = (await conn.execute(text("""SELECT max_hp FROM damage_recon_boss
                WHERE realm=:realm AND clan_id=:clan AND battle_id=:battle
                AND lap=:lap AND boss=:boss"""), dict(realm=realm, clan=clan_id,
                battle=battle_id, lap=lap, boss=boss))).scalar()
            source = "server_snapshot" if hp else "period_config"
            hp = hp or config_hp
            if not isinstance(hp, int) or hp <= 0:
                return
            await conn.execute(text("""INSERT OR IGNORE INTO damage_recon_job
                (job_key,realm,clan_id,battle_id,period,lap,boss,max_hp,hp_source,
                 created,updated,due,kill_key)
                VALUES (:key,:realm,:clan,:battle,:period,:lap,:boss,:hp,:source,
                        :now,:now,:now,:kill)"""), dict(key=key, realm=realm,
                clan=clan_id, battle=battle_id, period=period or 0, lap=lap,
                boss=boss, hp=hp, source=source, now=now,
                kill=encode(kill_key) if kill_key else None))
            groups = group_id if isinstance(group_id, (list, tuple)) else [group_id]
            if not groups or len(groups) > 99:
                raise ValueError("invalid_subscription_batch")
            await conn.execute(text("""INSERT OR IGNORE INTO damage_recon_group
                (job_key,group_id) VALUES (:key,:group)"""),
                [dict(key=key, group=group) for group in groups])
            if kill_key:
                # A later observation can supply kill evidence missing at enqueue.
                await conn.execute(text("""UPDATE damage_recon_job SET kill_key=:kill
                    WHERE job_key=:key AND kill_key IS NULL AND status='pending'"""),
                    dict(key=key, kill=encode(kill_key)))

    async def due(self, now):
        # Peek at future work too, so an idle worker can sleep until its due time.
        async with self.engine.connect() as conn:
            row = (await conn.execute(text("""SELECT * FROM damage_recon_job
                WHERE status='pending' ORDER BY due,job_key LIMIT 1"""))).mappings().first()
            return dict(row) if row else None

    async def update(self, key, **values):
        allowed = {"stage", "page", "cached_seq", "attempts", "expected", "clear_time", "reason",
                   "status", "due", "result"}
        if not values or not set(values) <= allowed:
            raise ValueError("invalid task update")
        values["updated"] = int(time.time())
        sql = ",".join(name + "=:" + name for name in values)
        async with self.engine.begin() as conn:
            await conn.execute(text("UPDATE damage_recon_job SET " + sql + " WHERE job_key=:key"),
                               dict(values, key=key))

    async def reset(self, key, **values):
        async with self.engine.begin() as conn:
            await conn.execute(text("""DELETE FROM damage_recon_report WHERE rowid IN (
                SELECT rowid FROM damage_recon_report WHERE job_key=:key LIMIT 100)"""), dict(key=key))
            remaining = (await conn.execute(text("SELECT 1 FROM damage_recon_report WHERE job_key=:key LIMIT 1"),
                                            dict(key=key))).first()
            values.update(stage="reset" if remaining else "before", page=1, cached_seq=0,
                          updated=int(time.time()))
            if not set(values) <= {"stage", "page", "cached_seq", "updated", "attempts", "reason", "due"}:
                raise ValueError("invalid reset")
            sql = ",".join(name + "=:" + name for name in values)
            await conn.execute(text("UPDATE damage_recon_job SET " + sql + " WHERE job_key=:key"),
                               dict(values, key=key))

    async def add_page(self, job, records):
        values = []
        for r in records:
            if r.lap_num != job["lap"] or r.order_num != job["boss"]:
                continue
            if any(not isinstance(v, int) or v < 0 for v in (
                    r.target_viewer_id, r.battle_log_id, r.battle_end_time, r.total_damage)):
                raise ValueError("invalid_report")
            values.append(dict(key=job["job_key"], viewer=r.target_viewer_id,
                               report=r.battle_log_id, end=r.battle_end_time, damage=r.total_damage))
        async with self.engine.begin() as conn:
            for start in range(0, len(values), 100):
                # Network pages are small; guard against a surprising server payload.
                if start >= 100:
                    raise ValueError("oversized_page")
                batch = values[start:start + 100]
                params = {"key": job["job_key"]}
                pairs = []
                for i, value in enumerate(batch):
                    params["v%s" % i] = value["viewer"]
                    params["r%s" % i] = value["report"]
                    pairs.append("(:v%s,:r%s)" % (i, i))
                old_rows = (await conn.execute(text("""SELECT viewer_id,report_id,end_time,damage
                    FROM damage_recon_report WHERE job_key=:key AND (viewer_id,report_id) IN ("""
                    + ",".join(pairs) + ")"), params)).all()
                old_values = {(r[0], r[1]): (r[2], r[3]) for r in old_rows}
                for value in batch:
                    old = old_values.get((value["viewer"], value["report"]))
                    if old and old != (value["end"], value["damage"]):
                        raise ValueError("report_changed")
                    old_values[(value["viewer"], value["report"])] = (value["end"], value["damage"])
                await conn.execute(text("""INSERT OR IGNORE INTO damage_recon_report
                    VALUES (:key,:viewer,:report,:end,:damage)"""), values[start:start + 100])
            return (await conn.execute(text("SELECT count(*) FROM damage_recon_report WHERE job_key=:key"),
                                       dict(key=job["job_key"]))).scalar()

    async def evaluate(self, job, kill_histories):
        """Aggregate in SQLite rather than loading a boss's entire history."""
        key = job["job_key"]
        async with self.engine.connect() as conn:
            aggregate = (await conn.execute(text("""SELECT count(*) AS n,sum(damage) AS total,
                max(end_time) AS latest FROM damage_recon_report WHERE job_key=:key"""), dict(key=key))).mappings().one()
            if aggregate["n"] != job["expected"]:
                raise ValueError("attack_count_mismatch")
            duplicate = (await conn.execute(text("""SELECT 1 FROM damage_recon_report WHERE job_key=:key
                GROUP BY viewer_id,end_time HAVING count(*)>1 LIMIT 1"""), dict(key=key))).first()
            if duplicate:
                raise ValueError("ambiguous_report_key")
            kills = {(h.viewer_id, h.create_time) for h in kill_histories
                     if h.kill and h.lap_num == job["lap"] and h.order_num == job["boss"]}
            if job["kill_key"]:
                kills.add(tuple(json.loads(job["kill_key"])))
            if len(kills) > 1:
                raise ValueError("multiple_kills")
            if kills:
                viewer, timestamp = next(iter(kills))
                tail = (await conn.execute(text("""SELECT * FROM damage_recon_report
                    WHERE job_key=:key AND viewer_id=:viewer AND end_time=:end"""),
                    dict(key=key, viewer=viewer, end=timestamp))).mappings().first()
            else:
                # A unique final report matching server clear_time is also evidence.
                tails = (await conn.execute(text("""SELECT * FROM damage_recon_report
                    WHERE job_key=:key AND end_time=:end LIMIT 2"""),
                    dict(key=key, end=job["clear_time"]))).mappings().all()
                tail = tails[0] if len(tails) == 1 else None
            if not tail or tail["end_time"] != aggregate["latest"]:
                raise ValueError("missing_or_ambiguous_kill")
            cap = job["max_hp"] - (aggregate["total"] - tail["damage"])
            if not 0 < cap <= tail["damage"]:
                raise ValueError("hp_contradiction")
            return dict(viewer_id=tail["viewer_id"], end_time=tail["end_time"],
                        report_id=tail["report_id"], raw_damage=tail["damage"], damage=cap,
                        max_hp=job["max_hp"], hp_source=job["hp_source"],
                        attack_count=aggregate["n"], clear_time=job["clear_time"],
                        previous_damage=aggregate["total"] - tail["damage"], job_key=key)

    async def apply_one(self, now):
        """One subscribed group per turn; retry missing detailed reports later."""
        async with self.engine.begin() as conn:
            job = (await conn.execute(text("""SELECT j.*,g.group_id,g.last_report FROM damage_recon_job j
                JOIN damage_recon_group g ON g.job_key=j.job_key
                WHERE j.status='complete' AND g.applied=0 AND j.due<=:now
                ORDER BY j.due,j.job_key,g.group_id LIMIT 1"""), dict(now=now))).mappings().first()
            if not job:
                return None
            result = json.loads(job["result"])
            reports = (await conn.execute(text("""SELECT e.rowid AS evidence_id,e.viewer_id,
                e.report_id,e.end_time,e.damage AS raw,d.id,d.raw_damage,d.damage,d.correction_method
                FROM (SELECT rowid,* FROM damage_recon_report WHERE job_key=:key AND rowid>:cursor
                      ORDER BY rowid LIMIT 99) e
                LEFT JOIN recorddao d ON d.group_id=:group AND d.lap=:lap AND d.boss=:boss
                AND d.pcrid=e.viewer_id AND d.time=e.end_time
                AND (d.clan_battle_id=:battle OR d.clan_battle_id IS NULL)
                ORDER BY e.rowid LIMIT 199"""),
                dict(key=job["job_key"], cursor=job["last_report"], group=job["group_id"],
                     lap=job["lap"], boss=job["boss"], battle=job["battle_id"]))).mappings().all()
            if reports:
                seen = set()
                updates = []
                reason = ""
                for report in reports:
                    if report["evidence_id"] in seen:
                        reason = "ambiguous_report"
                        break
                    seen.add(report["evidence_id"])
                    if report["id"] is None:
                        reason = "missing_report"
                        break
                    is_tail = (report["viewer_id"] == result["viewer_id"]
                               and report["end_time"] == result["end_time"])
                    if ((report["raw_damage"] is not None and report["raw_damage"] != report["raw"])
                            or not is_tail and report["damage"] != report["raw"]):
                        reason = "raw_or_previous_damage_conflict"
                        break
                    updates.append(dict(raw=report["raw"], battle=job["battle_id"],
                                        report=report["report_id"], id=report["id"]))
                if reason:
                    applied = -1 if reason != "missing_report" or now - job["created"] >= 86400 else 0
                    await conn.execute(text("""UPDATE damage_recon_group SET applied=:applied,
                        reason=:reason WHERE job_key=:key AND group_id=:group"""),
                        dict(key=job["job_key"], group=job["group_id"], applied=applied, reason=reason))
                    await conn.execute(text("UPDATE damage_recon_job SET due=:due WHERE job_key=:key"),
                                       dict(key=job["job_key"], due=now + 30))
                    return dict(group_id=job["group_id"], applied=applied, job_key=job["job_key"])
                await conn.execute(text("""UPDATE recorddao SET raw_damage=:raw,
                    clan_battle_id=:battle,server_battle_log_id=:report WHERE id=:id"""), updates)
                await conn.execute(text("""UPDATE damage_recon_group SET last_report=:cursor
                    WHERE job_key=:key AND group_id=:group"""),
                    dict(key=job["job_key"], group=job["group_id"], cursor=reports[-1]["evidence_id"]))
                return dict(group_id=job["group_id"], applied=0, job_key=job["job_key"])
            args = dict(group=job["group_id"], lap=job["lap"], boss=job["boss"],
                        viewer=result["viewer_id"], end=result["end_time"], battle=job["battle_id"])
            rows = (await conn.execute(text("""SELECT id,damage,raw_damage,clan_battle_id,
                correction_method,correction_evidence FROM recorddao WHERE group_id=:group AND lap=:lap AND boss=:boss
                AND pcrid=:viewer AND time=:end AND (clan_battle_id=:battle OR clan_battle_id IS NULL)
                LIMIT 2"""), args)).mappings().all()
            applied = 0
            if len(rows) > 1:
                applied = -1
            elif rows:
                row = rows[0]
                # A previously corrected value must agree; never silently replace it.
                conflict = (row["correction_method"] and row["damage"] != result["damage"]
                            or row["raw_damage"] is not None and row["raw_damage"] != result["raw_damage"]
                            or row["damage"] not in (result["raw_damage"], result["damage"]))
                if conflict:
                    applied = -1
                else:
                    evidence = dict(result)
                    if row["correction_method"] == "snapshot":
                        evidence["snapshot"] = json.loads(row["correction_evidence"])
                    await conn.execute(text("""UPDATE recorddao SET raw_damage=:raw,damage=:damage,
                        clan_battle_id=:battle,server_battle_log_id=:report,correction_method='boss_total',
                        correction_evidence=:evidence,corrected_at=:now WHERE id=:id"""),
                        dict(raw=result["raw_damage"], damage=result["damage"], battle=job["battle_id"],
                             report=result["report_id"], evidence=encode(evidence), now=int(now), id=row["id"]))
                    applied = 1
            # Stop retrying a missing row after the task's original 24h window.
            if not applied and now - job["created"] >= 86400:
                applied = -1
            await conn.execute(text("""UPDATE damage_recon_group SET applied=:applied,reason=:reason
                WHERE job_key=:key AND group_id=:group"""),
                dict(key=job["job_key"], group=job["group_id"], applied=applied,
                     reason="record_conflict" if applied == -1 else ""))
            await conn.execute(text("UPDATE damage_recon_job SET due=:due WHERE job_key=:key"),
                               dict(key=job["job_key"], due=now + (0 if applied else 30)))
            return dict(group_id=job["group_id"], applied=applied, job_key=job["job_key"])

    async def cleanup(self, now):
        async with self.engine.begin() as conn:
            # At most 100 rows per table per sweep, even after long downtime.
            for table, predicate in (
                ("damage_recon_report", "job_key IN (SELECT job_key FROM damage_recon_job WHERE created<:cutoff)"),
                ("damage_recon_group", "job_key IN (SELECT job_key FROM damage_recon_job WHERE created<:cutoff)"),
                ("damage_recon_boss", "updated<:cutoff"),
            ):
                await conn.execute(text("DELETE FROM " + table + " WHERE rowid IN (SELECT rowid FROM "
                    + table + " WHERE " + predicate + " LIMIT 100)"), dict(cutoff=now - 30 * 86400))
            await conn.execute(text("""DELETE FROM damage_recon_job WHERE job_key IN (
                SELECT job_key FROM damage_recon_job j WHERE created<:cutoff
                AND NOT EXISTS(SELECT 1 FROM damage_recon_report r WHERE r.job_key=j.job_key)
                AND NOT EXISTS(SELECT 1 FROM damage_recon_group g WHERE g.job_key=j.job_key) LIMIT 100)"""),
                dict(cutoff=now - 30 * 86400))
