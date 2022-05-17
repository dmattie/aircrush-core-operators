from enum import Enum

class Readiness(Enum):
    NOT_READY='notready'
    LIMITED='limited'
    READY='ready'