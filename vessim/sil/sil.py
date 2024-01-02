import json
import multiprocessing
import pickle
import time
from collections import defaultdict
from threading import Thread
from typing import Dict, Callable, Optional

import docker
import redis
import uvicorn
from docker.models.containers import Container
from fastapi import FastAPI

from vessim import TimeSeriesApi
from vessim.core.microgrid import Microgrid
from vessim.cosim.controller import Controller
from vessim.cosim.util import Clock


class SilController(Controller):

    def __init__(
        self,
        step_size: int,
        api_routes: Callable,
        set_request_collector: Callable,
        set_request_collector_interval: float = 1,
        api_host: str = "127.0.0.1",
        api_port: int = 8000,
    ):
        super().__init__(step_size=step_size)
        self.api_routes = api_routes
        self.set_request_collector = set_request_collector
        self.set_request_collector_interval = set_request_collector_interval
        self.api_host = api_host
        self.api_port = api_port
        self.redis_docker_container = _redis_docker_container()
        self.redis_db = redis.Redis()

        self.microgrid = None
        self.clock = None
        self.grid_signals = None
        self.api_server_process = None

    def start(self, microgrid: Microgrid, clock: Clock, grid_signals: Dict):
        self.microgrid = microgrid
        self.clock = clock
        self.grid_signals = grid_signals

        self.api_server_process = multiprocessing.Process(  # TODO logging
            target=_serve_api,
            name="Vessim API",
            daemon=True,
            kwargs=dict(
                api_routes=self.api_routes,
                api_host=self.api_host,
                api_port=self.api_port,
                grid_signals=grid_signals,
            )
        )
        self.api_server_process.start()

        Thread(target=self._collect_set_requests_loop, daemon=True).start()

    def step(self, time: int, p_delta: float, actors: Dict) -> None:
        pipe = self.redis_db.pipeline()
        pipe.set("time", time)
        pipe.set("p_delta", p_delta)
        pipe.set("actors", json.dumps(actors))
        pipe.set("microgrid", self.microgrid.pickle())
        pipe.execute()

    def finalize(self) -> None:
        print("Shutting down Redis...")  # TODO logging
        if self.redis_docker_container is not None:
            self.redis_docker_container.stop()

    def _collect_set_requests_loop(self):
        while True:
            events = self.redis_db.lrange("set_events", start=0, end=-1)
            if len(events) > 0:
                events = [pickle.loads(e) for e in events]
                events_by_type = defaultdict(list)
                for event in events:
                    key = event["key"]
                    del event["key"]
                    events_by_type[key].append(event)
                self.set_request_collector(events_by_type, self.microgrid)
            self.redis_db.delete("set_events")
            time.sleep(self.set_request_collector_interval)


def _serve_api(
    api_routes: Callable,
    api_host: str,
    api_port: int,
    grid_signals: Dict[str, TimeSeriesApi],
):
    print("Starting API server...")
    app = FastAPI()
    api_routes(app, grid_signals, redis.Redis())
    config = uvicorn.Config(app=app, host=api_host, port=api_port, access_log=False)
    server = uvicorn.Server(config=config)
    server.run()


def _redis_docker_container(
    docker_client: Optional[docker.DockerClient] = None,
    port: int = 6379
) -> Container:
    """Initializes Docker client and starts Docker container with Redis."""
    if docker_client is None:
        try:
            docker_client = docker.from_env()
        except docker.errors.DockerException as e:
            raise RuntimeError("Could not connect to Docker.") from e
    container = docker_client.containers.run(
        "redis:latest",
        ports={f"6379/tcp": port},
        detach=True,  # run in background
    )

    # Check if the container has started
    while True:
        container_info = docker_client.containers.get(container.id)
        if container_info.status == "running":
            break
        time.sleep(1)

    return container