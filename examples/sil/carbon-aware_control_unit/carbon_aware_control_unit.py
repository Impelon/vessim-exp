import time
from copy import deepcopy
from vessim.sil.http_client import HTTPClient, HTTPClientError
from vessim.sil.stoppable_thread import StoppableThread
from threading import Thread, Lock
from typing import Dict, Optional


class RemoteBattery:
    """Initializes a battery instance that holds info of the remote battery.

    Args:
        soc: The initial state of the battery's state of charge in %.
        min_soc: The minimum state of charge threshold for the battery in %.
        grid_charge: The power which the battery is charged with from the
            public grid in W.
    """

    def __init__(self, soc: float = 0.0, min_soc: float = 0.0, grid_charge: float = 0.0) -> None:
        self.soc = soc
        self.min_soc = min_soc
        self.grid_charge = grid_charge


class CarbonAwareControlUnit:
    """The CACU uses the VESSIM API for real-time carbon-aware scenarios.

    The Carbon Aware control unit uses an API server to communicate with the
    VES simulation and retrieve real-time data about energy demand, solar power
    production, and grid carbon intensity via GET requests. Under predefined
    scenarios, the control unit sends SET requests to adjust the VES simulation
    and computing system behavior. The Carbon Aware control unit's objective is
    to optimize the use of renewable energy sources and minimize carbon
    emissions by taking real-time decisions and actions based on these
    scenarios.

    Args:
        server_address: The address of the server to connect to.
        nodes: A dictionary representing the nodes that the Control Unit
            manages, with node IDs as keys and node objects as values.

    Attributes:
        power_modes: The list of available power modes for the nodes.
        nodes: A dictionary representing the nodes that the Control
            Unit manages, with node IDs as keys and node objects as values.
        client: The HTTPClient object used to communicate with the server.
    """

    def __init__(self, server_address: str, nodes: dict) -> None:
        self.power_modes = ["power-saving", "normal", "high performance"]
        self.nodes = nodes
        self.client = HTTPClient(server_address)
        self.nodes_power_mode_old = {}
        self.battery = RemoteBattery()
        self.battery_old = self.battery
        self.ci = 0.0
        self.solar = 0.0
        self.lock = Lock()

        # Wait until vessim api server has started
        while True:
            try:
                self.client.get('/api/ci')
                break
            except HTTPClientError:
                time.sleep(1)

    def run_scenario(self, rt_factor: float, update_interval: Optional[float]):
        if update_interval is None:
            update_interval = rt_factor
        update_thread = StoppableThread(self._update_getter, update_interval)
        update_thread.start()

        current_time = 0
        while True:
            self.lock.acquire()
            self.scenario_step(current_time)
            self.lock.release()
            current_time += rt_factor
            time.sleep(rt_factor)

    def _update_getter(self) -> None:
        self.lock.acquire()
        value = self.client.get("/api/battery-soc")["battery_soc"]
        if value:
            self.battery.soc = value
        value = self.client.get("/api/solar")["solar"]
        if value:
            self.solar = value
        value = self.client.get("/api/ci")["ci"]
        if value:
            self.ci = value
        self.lock.release()

    def scenario_step(self, current_time) -> None:
        """A Carbon-Aware Scenario.

        Scenario step for the Carbon-Aware Control Unit. This process updates
        the Control Unit's values, sets the battery's minimum state of charge
        (SOC) based on the current time, and adjusts the power modes of the
        nodes based on the current carbon intensity and battery SOC.

        Args:
            current_time: Current simulation time.
        """
        nodes_power_mode = {}

        # Set the minimum SOC of the battery based on the current time
        if current_time < 60*5:
            self.battery.min_soc = 0.3
        else:
            self.battery.min_soc = 0.6

        # Adjust the power modes of the nodes based on the current carbon
        # intensity and battery SOC
        if self.ci <= 200 or self.battery.soc > 0.8:
            nodes_power_mode[self.nodes["gcp"]] = "high performance"
            #nodes_power_mode[self.nodes["raspi"]] = "high performance"
        elif self.ci >= 250 and self.battery.soc < self.battery.min_soc:
            nodes_power_mode[self.nodes["gcp"]] = "power-saving"
            #nodes_power_mode[self.nodes["raspi"]] = "power-saving"
        else:
            nodes_power_mode[self.nodes["gcp"]] = "normal"
            #nodes_power_mode[self.nodes["raspi"]] = "normal"

        # Send and forget if values changed
        if (self.battery.min_soc != self.battery_old.min_soc or
            self.battery.grid_charge != self.battery_old.grid_charge):
            Thread(target=self.send_battery, args=(self.battery,)).start()
            self.battery_old = deepcopy(self.battery)

        for node in self.nodes:
            if (
                not self.nodes_power_mode_old or
                node.id not in self.nodes_power_mode_old or
                nodes_power_mode[node.id] != self.nodes_power_mode_old[node.id]
            ):
                Thread(
                    target=self.send_node_power_mode,
                    args=(node.id, nodes_power_mode[node.id])
                ).start()

    def send_battery(self, battery: RemoteBattery) -> None:
        """Sends battery data to the VES API.

        Args:
            battery: An object containing the battery data to be sent.
        """
        try:
            self.client.put("/api/battery", {
                "min_soc": battery.min_soc,
                "grid_charge": battery.grid_charge
            })
        except HTTPClientError:
            print(f"Could not update battery.")

    def send_node_power_mode(self, node_id: int, power_mode: str) -> None:
        """Sends power mode data to the VES API.

        Args:
            node_id: The ID of the node.
            power_mode: The power mode of the node to be set.

        """
        try:
            self.client.put(f"/api/nodes/{node_id}", {"power_mode": power_mode})
        except HTTPClientError:
            print(f"Could not update power mode of node {node_id}.")
