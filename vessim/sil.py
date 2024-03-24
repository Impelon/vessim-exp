"""Vessim Software-in-the-Loop (SiL) components.

This module is still experimental, the public API might change at any time.
"""

from __future__ import annotations

import json
import multiprocessing
import pickle
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timedelta
from threading import Thread
from time import sleep
from typing import Any, Optional, Callable

import pandas as pd
import requests
import uvicorn
from fastapi import FastAPI
from loguru import logger
from requests.auth import HTTPBasicAuth

from vessim.cosim import Controller, Microgrid
from vessim.signal import Signal
from vessim._util import DatetimeLike

class Broker:
    def __init__(self, db_file: str = None):
        if db_file is None:
            # NOTE: An in-memory database will not work across multiple processes, only multiple threads.
            # As such, a temporary file is the only real alternative.
            db_file = Path("/tmp") / "vessim-sil-{:x}.db".format(id(self))
        self.db_file = Path(db_file).resolve()
        # TODO Possibly check concurreny level of sqlite3.
        # See: https://docs.python.org/3.10/library/sqlite3.html#sqlite3.threadsafety
        self._initialize_tables()

    @contextmanager
    def open_db(self):
        connection = sqlite3.connect(self.db_file, isolation_level="DEFERRED", timeout=30)
        try:
            yield connection
            connection.commit()
        except Exception as e:
            connection.rollback()
            raise e
        finally:
            connection.close()

    def _initialize_tables(self):
        with self.open_db() as c:
            tables = [ # table name, custom table columns
                ("microgrids", "microgrid pickleblob"),
                ("actor_infos", "name TEXT, info pickleblob"),
                ("p_deltas", "p_delta REAL"),
                ("events", "category TEXT, value pickleblob"),
            ]
            indices = [ # index name, name of affected table, columns to index
                ("idx_name", "actor_infos", "name"),
            ]
            # An extra column for the timestamp as an integer is used for fast queries.
            for definition in tables:
                indices.append(("idx_unixts", definition[0], "unixts DESC"))

            for definition in tables:
                c.execute("CREATE TABLE IF NOT EXISTS {} (unixts INT, exactts TEXT, {});".format(*definition))
            for definition in indices:
                c.execute("CREATE INDEX IF NOT EXISTS {} ON {} ({});".format(*definition))

    def _fetch_obj(self, column_spec: str, from_spec: str, converter: Optional[Callable[..., Any]] = None) -> Optional[Any]:
        # NOTE: Parameter substitution can unfortunately not be used to specify column or table names.
        with self.open_db() as c:
            result = c.execute("SELECT {} FROM {} ORDER BY unixts DESC LIMIT 1;".format(column_spec, from_spec)).fetchone()
        if result is None or converter is None:
            return result
        return converter(*result)

    def get_microgrid(self) -> Optional[Microgrid]:
        return self._fetch_obj("microgrid", "microgrids", pickle.loads)  # type: ignore

    def get_actor(self, actor: str) -> Optional[dict]:
        return self._fetch_obj("info", "actor_infos WHERE name = {}".format(actor), pickle.loads)  # type: ignore

    def get_p_delta(self) -> Optional[float]:
        return self._fetch_obj("p_delta", "p_deltas", float)  # type: ignore

    def set_event(self, category: str, value: Any) -> None:
        time = datetime.now()
        params = (int(time.timestamp()), time.isoformat())
        with self.open_db() as c:
            c.execute("INSERT INTO events VALUES (?,?,?,?)", params + (category, pickle.dumps(value)))

    def _add_microgrid_data(self, time: datetime, data: dict) -> None:
        params = (int(time.timestamp()), time.isoformat())
        with self.open_db() as c:
            import sys
            if "microgrid" in data:
                c.execute("INSERT INTO microgrids VALUES (?,?,?)", params + (pickle.dumps(data["microgrid"]),))
            if "actor_infos" in data:
                for name, info in data["actor_infos"].items():
                    c.execute("INSERT INTO actor_infos VALUES (?,?,?,?)", params + (name, pickle.dumps(info)))
            if "p_delta" in data:
                c.execute("INSERT INTO p_deltas VALUES (?,?,?)", params + (data["p_delta"],))

    def _consume_events(self) -> Iterable[dict]:
        results = []
        with self.open_db() as c:
            c = c.execute("BEGIN TRANSACTION;")
            results = c.execute("SELECT exactts, category, value FROM events;").fetchall()
            c.execute("DELETE FROM events;")
        for result in results:
            yield {
                "time": datetime.fromisoformat(result[0]),
                "category": result[1],
                "value": pickle.loads(result[2]),
            }

    def finalize(self) -> None:
        if str(self.db_file).startswith("/tmp"):
            self.db_file.unlink(missing_ok=True)


class SilController(Controller):
    def __init__(
        self,
        api_routes: Callable,
        grid_signals: Optional[list[Signal]] = None,  # TODO temporary fix
        request_collectors: Optional[dict[str, Callable]] = None,
        api_host: str = "127.0.0.1",
        api_port: int = 8000,
        request_collector_interval: float = 1,
        step_size: Optional[int] = None,
        **kwargs,
    ):
        super().__init__(step_size=step_size)
        self.api_routes = api_routes
        self.grid_signals = grid_signals
        self.request_collectors = request_collectors if request_collectors is not None else {}
        self.api_host = api_host
        self.api_port = api_port
        self.request_collector_interval = request_collector_interval
        self.kwargs = kwargs
        self.broker = Broker()

        self.microgrid: Optional[Microgrid] = None

    def start(self, microgrid: Microgrid) -> None:
        self.microgrid = microgrid
        name = "Vessim API for microgrid {:x}".format(id(self.microgrid))

        multiprocessing.Process(
            target=_serve_api,
            name=name,
            daemon=True,
            kwargs=dict(
                api_routes=self.api_routes,
                api_host=self.api_host,
                api_port=self.api_port,
                broker=self.broker,
                grid_signals=self.grid_signals,
            ),
        ).start()
        logger.info("Started SiL Controller API server process '{}'", name)

        Thread(target=self._collect_set_requests_loop, daemon=True).start()

    def step(self, time: datetime, p_delta: float, actor_infos: dict) -> None:
        assert self.microgrid is not None
        self.broker._add_microgrid_data(time, {
            "microgrid": self.microgrid,
            "actor_infos": actor_infos,
            "p_delta": p_delta,
        })

    def finalize(self) -> None: # TODO-now stop daemon processes?
        self.broker.finalize()

    def _collect_set_requests_loop(self):
        while True:
            events_by_category = defaultdict(dict)
            for event in self.broker._consume_events():
                events_by_category[event["category"]][event["time"]] = event["value"]
            for category, events in events_by_category.items():
                self.request_collectors[category](
                    events=events_by_category[category],
                    microgrid=self.microgrid,
                    kwargs=self.kwargs,
                )
            sleep(self.request_collector_interval)


def _serve_api(
    api_routes: Callable,
    api_host: str,
    api_port: int,
    broker: Broker,
    grid_signals: dict[str, Signal],
):
    app = FastAPI()
    api_routes(app, broker, grid_signals)
    config = uvicorn.Config(app=app, host=api_host, port=api_port, access_log=False)
    server = uvicorn.Server(config=config)
    server.run()

def get_latest_event(events: dict[datetime, Any]) -> Any:
    return events[max(events.keys())]


class WatttimeSignal(Signal):
    _URL = "https://api.watttime.org"

    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.headers = {"Authorization": f"Bearer {self._login()}"}

    def at(
        self,
        dt: DatetimeLike,
        region: Optional[str] = None,
        signal_type: str = "co2_moer",
        **kwargs,
    ):
        if region is None:
            raise ValueError("Region needs to be specified.")
        dt = pd.to_datetime(dt)
        rsp = self._request(
            "/historical",
            params={
                "region": region,
                "start": (dt - timedelta(minutes=5)).isoformat(),
                "end": dt.isoformat(),
                "signal_type": signal_type,
            },
        )
        return rsp

    def _request(self, endpoint: str, params: dict):
        while True:
            rsp = requests.get(f"{self._URL}/v3{endpoint}", headers=self.headers, params=params)
            if rsp.status_code == 200:
                return rsp.json()["data"][0]["value"]
            if rsp.status_code == 400:
                return f"Error {rsp.status_code}: {rsp.json()}"
            elif rsp.status_code in [401, 403]:
                print("Renewing authorization with Watttime API.")
                self.headers["Authorization"] = f"Bearer {self._login()}"
            else:
                raise ValueError(f"Error {rsp.status_code}: {rsp}")

    def _login(self) -> str:
        # TODO reconnect if token is expired
        rsp = requests.get(f"{self._URL}/login", auth=HTTPBasicAuth(self.username, self.password))
        return rsp.json()["token"]
