"""
Requisiti: n > 5f (BFT ottimale) o n > 2f (Crash).
"""
import asyncio
import dataclasses
import enum
import logging
import random
import time
import heapq
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("BenOr")

class Network:
    def __init__(self, base_latency_ms=20, jitter_ms=20, drop_prob=0.0, seed=42):
        self.inboxes = {}
        self.base_latency_ms = base_latency_ms
        self.jitter_ms = jitter_ms
        self.drop_prob = drop_prob
        self._pq = []
        self._seq = 0
        self._running = True
        self.msg_count = 0
        self._rand = random.Random(seed)
        self._task = asyncio.create_task(self._run())

    def register(self, pid):
        q = asyncio.Queue()
        self.inboxes[pid] = q
        return q
    
    async def stop(self):
        self._running = False
        await self._task

    async def _run(self):
        while self._running:
            now = time.time()
            if self._pq and self._pq[0][0] <= now:
                _, _, dst, payload = heapq.heappop(self._pq)
                if dst in self.inboxes:
                    await self.inboxes[dst].put(payload)
                continue
            await asyncio.sleep(0.001)

    def send(self, src, dst, payload):
        self.msg_count += 1
        if self._rand.random() < self.drop_prob: return
        
        latency = self.base_latency_ms + self._rand.randint(0, max(0, self.jitter_ms))
        deliver_at = time.time() + latency / 1000.0
        
        self._seq += 1
        heapq.heappush(self._pq, (deliver_at, self._seq, dst, payload))

class MsgType(enum.Enum):
    FIRST = "FIRST"    # Round 1
    SECOND = "SECOND"  # Round 2

@dataclasses.dataclass(frozen=True)
class BenOrMessage:
    mtype: MsgType
    src: str
    phase: int
    value: Optional[int]

class BenOrNode:
    def __init__(self, node_id: str, all_nodes: List[str], network: Network, initial_value: int):
        self.id = node_id
        self.all_nodes = all_nodes
        self.N = len(all_nodes)
        self.f = (self.N - 1) // 2 # Soglia standard n > 2f
        self.network = network
        self.inbox = network.register(node_id)
        
        self.x = initial_value
        self.phase = 1
        self.decided = False
        self.decision = None
        
        self.future_msg_buffer = defaultdict(lambda: defaultdict(list))
        self._stop = False
        self._task = None

    def start(self):
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._stop = True
        if self._task: await self._task

    def broadcast(self, mtype: MsgType, val: Optional[int]):
        msg = BenOrMessage(mtype, self.id, self.phase, val)
        for dst in self.all_nodes:
            self.network.send(self.id, dst, msg)

    async def _collect_messages(self, target_phase: int, mtype: MsgType, required_count: int) -> List[BenOrMessage]:
        collected = []
        senders_seen = set()

        if target_phase in self.future_msg_buffer and mtype in self.future_msg_buffer[target_phase]:
            buffered_msgs = self.future_msg_buffer[target_phase][mtype]
            while buffered_msgs:
                msg = buffered_msgs.pop(0)
                if msg.src not in senders_seen:
                    collected.append(msg)
                    senders_seen.add(msg.src)
                    if len(collected) >= required_count: return collected

        while len(collected) < required_count and not self._stop:
            try:
                msg: BenOrMessage = await asyncio.wait_for(self.inbox.get(), timeout=0.1)
                
                if msg.phase < target_phase: continue 

                if msg.phase > target_phase:
                    self.future_msg_buffer[msg.phase][msg.mtype].append(msg)
                    continue
                
                if msg.mtype != mtype:
                    self.future_msg_buffer[msg.phase][msg.mtype].append(msg)
                    continue

                if msg.src not in senders_seen:
                    collected.append(msg)
                    senders_seen.add(msg.src)

            except asyncio.TimeoutError:
                continue
        
        return collected

    async def _run(self):
        while not self._stop:
            #ROUND 1
            self.broadcast(MsgType.FIRST, self.x)
            msgs1 = await self._collect_messages(self.phase, MsgType.FIRST, self.N - self.f)
            if self._stop: break

            y = None
            counts1 = defaultdict(int)
            for m in msgs1: counts1[m.value] += 1
            
            threshold_r1 = (self.N // 2) + 1
            for val, count in counts1.items():
                if count >= threshold_r1:
                    y = val
                    break
            
            #ROUND 2
            self.broadcast(MsgType.SECOND, y)
            msgs2 = await self._collect_messages(self.phase, MsgType.SECOND, self.N - self.f)
            if self._stop: break

            vals_non_null = [m.value for m in msgs2 if m.value is not None]
            
            if vals_non_null:
                self.x = vals_non_null[0] 
            
            counts2 = defaultdict(int)
            for m in msgs2:
                if m.value is not None: counts2[m.value] += 1
            
            decided_in_round = False
            for val, count in counts2.items():
                if count >= self.f + 1:
                    if not self.decided:
                        self.decided = True
                        self.decision = val
                        logger.info(f"{self.id} DECIDED {val} at Phase {self.phase}")
                    self.x = val 
                    decided_in_round = True
                    break
            
            if not decided_in_round and not vals_non_null:
                self.x = random.choice([0, 1])

            self.phase += 1
            await asyncio.sleep(0) 

async def demo_benor(num_nodes=5, consensus_iterations=5, drop_prob=0.0, base_latency=20, jitter=20):
    logger.info(f"=== BEN-OR CONSENSUS (N={num_nodes}, K={consensus_iterations}, Drop={drop_prob}) ===")
    
    node_ids = [f"n{i}" for i in range(num_nodes)]
    total_start = time.time()
    total_msgs = 0
    
    for k in range(consensus_iterations):
        net = Network(base_latency_ms=base_latency, jitter_ms=jitter, drop_prob=drop_prob, seed=random.randint(0, 10000))
        
        inputs = [random.choice([0, 1]) for _ in range(num_nodes)]
        nodes = []
        for i, nid in enumerate(node_ids):
            n = BenOrNode(nid, node_ids, net, initial_value=inputs[i])
            nodes.append(n)
        
        for n in nodes: n.start()
        
        while True:
            done_count = sum(1 for n in nodes if n.decided)
            if done_count == num_nodes:
                decisions = set(n.decision for n in nodes)
                if len(decisions) > 1:
                    logger.error(f"FATAL: Safety violation! Decisions: {decisions}")
                break
            await asyncio.sleep(0.01)
        
        for n in nodes: await n.stop()
        await net.stop()
        total_msgs += net.msg_count

    total_end = time.time()
    elapsed = total_end - total_start
    tps = consensus_iterations / elapsed if elapsed > 0 else 0
    logger.info(f"RESULT -> Time: {elapsed:.4f}s | Msgs: {total_msgs} | TPS: {tps:.2f}")
    
    return elapsed, total_msgs, tps

if __name__ == "__main__":
    try:
        asyncio.run(demo_benor(num_nodes=5, consensus_iterations=10, drop_prob=0.0, base_latency=10, jitter=5))
    except KeyboardInterrupt: pass