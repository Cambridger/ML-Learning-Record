from abc import ABC
from dataclasses import dataclass
from typing import Dict
import requests

from bots.bot_template import CMIBot, STANDARD_HEADERS


@dataclass
class IMCBot(CMIBot, ABC):
    def __post_init__(self):
        all_users = requests.get(f"{self._cmi_url}/api/user")
        if all_users.status_code == 200:
            if not self.username in [user["username"] for user in all_users.json()]:
                self._register()
        super().__post_init__()

    @property
    def theos(self) -> Dict[str, float]:
        # TODO get actual theo for equator
        return {
            "EQUATOR": self.number_crossed_equator,
            "FLIGHTS": self.number_of_flights_in_air,
        }

    def _register(self):
        response = requests.post(
            url=f"{self._cmi_url}/api/user",
            json={"username": self.username, "password": self.password},
            headers=STANDARD_HEADERS,
        )
