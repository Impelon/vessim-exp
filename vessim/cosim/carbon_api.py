from datetime import datetime

from vessim.core.simulator import CarbonApi
from vessim.cosim._util import VessimSimulator, VessimModel, Clock


class CarbonApiSim(VessimSimulator):

    META = {
        'type': 'time-based',
        'models': {
            'CarbonApi': {
                'public': True,
                'params': ['zone'],
                'attrs': ['carbon_intensity'],
            },
        },
    }

    def __init__(self):
        super().__init__(self.META, _CarbonApiModel)
        self.clock = None
        self.sim_start = None
        self.carbon_api = None

    def init(self, sid, time_resolution, sim_start: datetime, carbon_api: CarbonApi,
             eid_prefix=None):
        super().init(sid, time_resolution, eid_prefix=eid_prefix)
        self.clock = Clock(sim_start)
        self.carbon_api = carbon_api
        return self.meta

    def create(self, num, model, zone: str):
        return super().create(num, model, zone=zone, clock=self.clock,
                              carbon_api=self.carbon_api)

    def next_step(self, time: int) -> int:
        dt = self.clock.to_datetime(time)
        next_dt = self.carbon_api.next_update(dt)
        return self.clock.to_simtime(next_dt)


class _CarbonApiModel(VessimModel):
    def __init__(self, carbon_api: CarbonApi, clock: Clock, zone: str):
        self.api = carbon_api
        self.clock = clock
        self.zone = zone
        self.carbon_intensity = None

    def step(self, time: int, inputs: dict) -> None:
        dt = self.clock.to_datetime(time)
        self.carbon_intensity = self.api.carbon_intensity_at(dt, self.zone)