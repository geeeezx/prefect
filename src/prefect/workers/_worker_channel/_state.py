from __future__ import annotations

import copy
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

import anyio.abc
import websockets.asyncio.client

from prefect.client.orchestration import PrefectClient
from prefect.client.schemas.objects import WorkPool
from prefect.client.schemas.worker_channel import (
    WorkerReadyFrame,
    WorkPoolSnapshotPayload,
)


class WorkerChannelStatus(str, Enum):
    CONNECTING = "connecting"
    HEALTHY = "healthy"
    FALLBACK_RETRYING = "fallback_retrying"
    DISABLED = "disabled"


class WorkerChannelError(Exception):
    pass


class WorkerChannelTerminalError(WorkerChannelError):
    def __init__(self, reason: str, message: str):
        super().__init__(message)
        self.reason = reason


class WorkerChannelRetryableError(WorkerChannelError):
    def __init__(self, reason: str, message: str):
        super().__init__(message)
        self.reason = reason


@dataclass
class WorkerChannelState:
    status: WorkerChannelStatus = WorkerChannelStatus.FALLBACK_RETRYING
    reason: str | None = None

    @property
    def rest_fallback_enabled(self) -> bool:
        return self.status != WorkerChannelStatus.HEALTHY

    @property
    def healthy(self) -> bool:
        return self.status == WorkerChannelStatus.HEALTHY

    @property
    def terminal(self) -> bool:
        return self.status == WorkerChannelStatus.DISABLED

    def mark_connecting(self) -> None:
        self.status = WorkerChannelStatus.CONNECTING
        self.reason = None

    def mark_healthy(self) -> None:
        self.status = WorkerChannelStatus.HEALTHY
        self.reason = None

    def mark_unhealthy(self, reason: str | None = None) -> None:
        self.status = WorkerChannelStatus.FALLBACK_RETRYING
        self.reason = reason

    def mark_terminal(self, reason: str | None = None) -> None:
        self.status = WorkerChannelStatus.DISABLED
        self.reason = reason


WorkerChannelFallbackState = WorkerChannelState


@dataclass
class WorkPoolSnapshotState:
    work_pool: WorkPool | None = None
    last_applied_sequence: int | None = None

    @property
    def snapshots_available(self) -> bool:
        return self.last_applied_sequence is not None

    def reset_connection_sequence(self) -> None:
        self.last_applied_sequence = None

    def apply_snapshot(self, snapshot: WorkPoolSnapshotPayload) -> WorkPool | None:
        if (
            self.last_applied_sequence is not None
            and snapshot.snapshot_sequence <= self.last_applied_sequence
        ):
            return None

        self.last_applied_sequence = snapshot.snapshot_sequence
        self.work_pool = copy.deepcopy(snapshot.work_pool)
        return self.work_pool

    def replace_from_rest(self, work_pool: WorkPool) -> WorkPool:
        self.work_pool = copy.deepcopy(work_pool)
        return self.work_pool


@dataclass
class WorkerChannelConnection:
    connect_context: websockets.asyncio.client.connect
    websocket: websockets.asyncio.client.ClientConnection
    ready: WorkerReadyFrame


class WorkerChannel(Protocol):
    @property
    def rest_fallback_enabled(self) -> bool: ...

    @property
    def snapshots_available(self) -> bool: ...

    def set_client(self, client: PrefectClient) -> None: ...

    async def sync(self, task_group: anyio.abc.TaskGroup | None) -> None: ...

    def stop(self) -> None: ...
