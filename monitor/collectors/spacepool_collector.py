from __future__ import annotations

import asyncio
import logging
from asyncio.queues import Queue
from datetime import datetime
from pathlib import Path
from typing import Dict

import aiohttp
from monitor.collectors.collector import Collector
from monitor.database import ChiaEvent
from monitor.database.events import SpacePoolFarmerEvent

LAUNCHER_ID = "0xCHANGEME"
FARMER_API = f"https://pool.space/api/farms/{LAUNCHER_ID}"


class SpacePoolCollector(Collector):
    session: aiohttp.ClientSession

    @staticmethod
    async def create(_root_path: Path, _net_config: Dict, event_queue: Queue[ChiaEvent]) -> Collector:
        self = SpacePoolCollector()
        self.log = logging.getLogger(__name__)
        self.event_queue = event_queue
        self.session = aiohttp.ClientSession()
        return self

    async def get_current_stat(self) -> None:
        async with self.session.get(FARMER_API) as resp:
            result = await resp.json()
            event = SpacePoolFarmerEvent(
                ts=datetime.now(),
                unpaid_balance_mojos=int(result['unpaidBalanceInXCH'] * 1e12),
                paid_balance_mojos=int(result['totalPaidInXCH'] * 1e12),
                total_points=int(result['totalPoints']),
                pending_points=int(result['pendingPoints']),
                global_pending_points=int(result['globalPendingPoints']),
                blocks_found=int(result['blocksFound']),
                estimated_plot_size=int(result['estimatedPlotSizeTiB'] * 1099511627776),
                estimated_plot_count=int(result['estimatedPlots']),
                difficulty=int(result['lastDifficulty']),
                rank=int(result['rank'])
            )
            await self.publish_event(event)

    async def task(self) -> None:
        while True:
            try:
                await self.get_current_stat()
            except Exception as e:
                self.log.warning(f"Error while collecting stat for space pool. Trying again... {type(e).__name__}: {e}")
            await asyncio.sleep(60)

    async def close(self) -> None:
        await self.session.close()
