import asyncio
import dataclasses
import enum
import logging
import random
import time
import hashlib
import heapq
from collections import defaultdict
from typing import Any, Dict, List, Set

from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes
from cryptography.exceptions import InvalidSignature

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("PBFT_Impl")

# Gestione della crittografia del nodo
class KeyManager:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self._private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.public_key = self._private_key.public_key()
        self.pki = {}
    def register_node(self, node_id: str, public_key): self.pki[node_id] = public_key
    def sign(self, message_bytes: bytes) -> bytes:
        return self._private_key.sign(message_bytes, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH), hashes.SHA256())
    def verify(self, node_id: str, message_bytes: bytes, signature: bytes) -> bool:
        if node_id not in self.pki: return False
        try:
            self.pki[node_id].verify(signature, message_bytes, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH), hashes.SHA256())
            return True
        except InvalidSignature: return False

# Simulazione di un Ambiente di rete asincrono con latenza configurabile
class Network:
    def __init__(self, *, base_latency_ms: int = 20, jitter_ms: int = 20, drop_prob: float = 0.0, seed: int = 42):
        self.base_latency_ms = base_latency_ms
        self.jitter_ms = jitter_ms
        self.drop_prob = drop_prob
        self.inboxes = {}
        self._pq = []
        self._seq = 0
        self._running = True
        self.msg_count = 0
        self._rand = random.Random(seed)
        asyncio.create_task(self._run())

    def register(self, pid): 
        q = asyncio.Queue()
        self.inboxes[pid] = q
        return q
    
    # Cuore della simulazione che smista i messaggi basandosi sul tempo di consegna
    async def _run(self):
        while self._running:
            now = time.time()
            if self._pq and self._pq[0][0] <= now:
                _, _, dst, payload = heapq.heappop(self._pq)
                if dst in self.inboxes: await self.inboxes[dst].put(payload)
                continue
            await asyncio.sleep(0.001)

    # Calcola la latenza e inserisce il messaggio nella queue di priorità
    def send(self, src, dst, payload):
        self.msg_count += 1
        if self._rand.random() < self.drop_prob: return
        
        # Calcolo latenza dinamica basata su base_latency e jitter
        latency = self.base_latency_ms + self._rand.randint(0, max(0, self.jitter_ms))
        deliver_at = time.time() + latency / 1000.0
        
        self._seq += 1
        heapq.heappush(self._pq, (deliver_at, self._seq, dst, payload))

class MsgType(enum.Enum):
    REQUEST = "REQUEST"
    PREPREPARE = "PREPREPARE"
    PREPARE = "PREPARE"
    COMMIT = "COMMIT"
    VIEW_CHANGE = "VIEW_CHANGE"
    NEW_VIEW = "NEW_VIEW"

@dataclasses.dataclass(frozen=True)
class BFTMessage:
    mtype: MsgType
    src: str
    view: int
    seq: int
    digest: str
    value: Any = None
    signature: bytes = b""
    def pack_for_signing(self) -> bytes:
        val_str = str(self.value) if self.value is not None else ""
        data = f"{self.mtype.value}:{self.view}:{self.seq}:{self.digest}:{val_str}"
        return data.encode('utf-8')

class ByzantineBehavior(enum.Enum):
    HONEST = "HONEST"
    SILENT = "SILENT"
    CRASH_AFTER_SEQ_1 = "CRASH_AFTER_SEQ_1"
    LIAR = "LIAR"

# Rappresentazione di un nodo PBFT
class BFTNode:
    def __init__(self, node_id: str, all_nodes: List[str], network: Network, behavior: ByzantineBehavior = ByzantineBehavior.HONEST):
        self.id = node_id
        self.all_nodes = sorted(all_nodes, key=lambda x: int(x[1:]))
        self.N = len(all_nodes)
        self.f = (self.N - 1) // 3
        self.quorum = 2 * self.f + 1
        self.network = network
        self.inbox = network.register(node_id)
        self.behavior = behavior
        self.km = KeyManager(node_id)

        self.view = 0
        self.seq = 0
        self.prepared_log = set()
        self.committed_log = set()
        self.executed_log = {} 
        self.val_cache = {} 

        self.prepare_votes = defaultdict(set)
        self.commit_votes = defaultdict(set)
        
        self.view_change_votes = defaultdict(set)
        self.in_view_change = False
        self.last_msg_time = time.time()
        self.timeout_interval = 5.0 

        self._stop = False
        self.sent_new_view = False
        self._t1 = None
        self._t2 = None

    def get_primary(self, view): return self.all_nodes[view % self.N]
    
    def start(self): 
        self._t1 = asyncio.create_task(self._run())
        self._t2 = asyncio.create_task(self._monitor_liveness())

    async def stop(self):
        self._stop = True
        if self._t1: self._t1.cancel()
        if self._t2: self._t2.cancel()
        try: await self._t1
        except: pass
        try: await self._t2
        except: pass

    # Loop principale di ricezione messaggi
    async def _run(self):
        while not self._stop:
            try:
                msg = await self.inbox.get()
                # Simulazione Crash Fault
                if self.behavior == ByzantineBehavior.CRASH_AFTER_SEQ_1 and self.seq >= 1:
                    if not getattr(self, '_crashed_log', False):
                        logger.critical(f"{self.id} CRASHING now (simulated)!")
                        self._crashed_log = True
                    continue
                if self.behavior == ByzantineBehavior.SILENT: continue
                await self._handle(msg)
            except asyncio.CancelledError: break

    # Monitoraggio della liveness del leader
    async def _monitor_liveness(self):
        while not self._stop:
            try:
                await asyncio.sleep(0.1)
                if self.get_primary(self.view) != self.id and not self.in_view_change:
                    if time.time() - self.last_msg_time > self.timeout_interval:
                        logger.warning(f"{self.id} DETECTED TIMEOUT from Primary. Initiating VIEW CHANGE.")
                        await self._start_view_change()
            except asyncio.CancelledError: break

    def broadcast(self, msg: BFTMessage):
        sig = self.km.sign(msg.pack_for_signing())
        signed_msg = dataclasses.replace(msg, signature=sig)
        for dst in self.all_nodes: self.network.send(self.id, dst, signed_msg)

    async def _handle(self, msg: BFTMessage):
        if not self.km.verify(msg.src, msg.pack_for_signing(), msg.signature): return
        if msg.src == self.get_primary(self.view) and msg.view == self.view: self.last_msg_time = time.time()

        if msg.mtype == MsgType.VIEW_CHANGE: await self._handle_view_change(msg)
        elif msg.mtype == MsgType.NEW_VIEW: await self._handle_new_view(msg)
        if self.in_view_change: return

        if msg.mtype == MsgType.REQUEST: await self._handle_request(msg)
        elif msg.mtype == MsgType.PREPREPARE: await self._handle_preprepare(msg)
        elif msg.mtype == MsgType.PREPARE: await self._handle_prepare(msg)
        elif msg.mtype == MsgType.COMMIT: await self._handle_commit(msg)

    async def _handle_request(self, msg: BFTMessage):
        if self.id != self.get_primary(self.view): return
        self.seq += 1
        digest = self._hash(msg.value)
        logger.info(f"{self.id} [PRIMARY v{self.view}] Consensing seq={self.seq} val={msg.value}")
        self.broadcast(BFTMessage(MsgType.PREPREPARE, self.id, self.view, self.seq, digest, value=msg.value))

    async def _handle_preprepare(self, msg: BFTMessage):
        if msg.view != self.view: return
        if msg.src != self.get_primary(self.view): return
        calc_digest = self._hash(msg.value)
        if msg.digest != calc_digest:
            logger.warning(f"{self.id} PREPREPARE digest mismatch!")
            return
        self.val_cache[msg.digest] = msg.value
        if msg.seq > self.seq: self.seq = msg.seq 
        if self.behavior == ByzantineBehavior.LIAR:
            fake = self._hash("FAKE")
            self.broadcast(BFTMessage(MsgType.PREPARE, self.id, self.view, msg.seq, fake))
            return 
        self.broadcast(BFTMessage(MsgType.PREPARE, self.id, self.view, msg.seq, msg.digest))
        self.prepare_votes[(msg.view, msg.seq, msg.digest)].add(msg.src)
        self.prepare_votes[(msg.view, msg.seq, msg.digest)].add(self.id)

    async def _handle_prepare(self, msg: BFTMessage):
        if msg.view != self.view: return
        key = (msg.view, msg.seq, msg.digest)
        self.prepare_votes[key].add(msg.src)
        if len(self.prepare_votes[key]) >= self.quorum and key not in self.prepared_log:
            self.prepared_log.add(key)
            if self.behavior == ByzantineBehavior.LIAR:
                fake = self._hash("FAKE")
                self.broadcast(BFTMessage(MsgType.COMMIT, self.id, self.view, msg.seq, fake))
                return
            self.broadcast(BFTMessage(MsgType.COMMIT, self.id, self.view, msg.seq, msg.digest))

    async def _handle_commit(self, msg: BFTMessage):
        if msg.view != self.view: return
        key = (msg.view, msg.seq, msg.digest)
        self.commit_votes[key].add(msg.src)
        if len(self.commit_votes[key]) >= self.quorum and key not in self.committed_log:
            self.committed_log.add(key)
            val = self.val_cache.get(msg.digest, f"HASH:{msg.digest[:8]}...")
            logger.info(f"{self.id} EXECUTED seq={msg.seq} (View {self.view})")
            self.executed_log[msg.seq] = val

    # Logica di View Change per garantire liveness dopo un crash
    async def _start_view_change(self):
        self.in_view_change = True
        next_view = self.view + 1
        logger.info(f"{self.id} VOTING for VIEW CHANGE -> v{next_view}")
        self.broadcast(BFTMessage(MsgType.VIEW_CHANGE, self.id, next_view, 0, ""))
        self.view_change_votes[next_view].add(self.id)

    async def _handle_view_change(self, msg: BFTMessage):
        target_view = msg.view
        if target_view <= self.view: return
        self.view_change_votes[target_view].add(msg.src)
        if self.get_primary(target_view) == self.id:
            count = len(self.view_change_votes[target_view])
            if count >= self.quorum and not self.sent_new_view:
                logger.info(f"{self.id} [NEW PRIMARY] Taking over!")
                self.sent_new_view = True
                self.broadcast(BFTMessage(MsgType.NEW_VIEW, self.id, target_view, 0, ""))
                await self._switch_view(target_view)

    async def _handle_new_view(self, msg: BFTMessage):
        if msg.src != self.get_primary(msg.view): return
        logger.info(f"{self.id} ACCEPTED NEW VIEW v{msg.view} from {msg.src}")
        await self._switch_view(msg.view)

    async def _switch_view(self, new_view):
        self.view = new_view
        self.in_view_change = False
        self.last_msg_time = time.time()
        self.sent_new_view = False

    def _hash(self, val: Any) -> str:
        return hashlib.sha256(str(val).encode()).hexdigest()

async def demo_safety():
    logger.info("=== DEMO: SAFETY (Resilience to Liar) ===")
    node_ids = ["n0", "n1", "n2", "n3"]
    net = Network(seed=42)
    nodes = [
        BFTNode("n0", node_ids, net, ByzantineBehavior.HONEST),
        BFTNode("n1", node_ids, net, ByzantineBehavior.HONEST),
        BFTNode("n2", node_ids, net, ByzantineBehavior.HONEST),
        BFTNode("n3", node_ids, net, ByzantineBehavior.LIAR)
    ]
    client_km = KeyManager("client")
    for n in nodes:
        for peer in nodes: n.km.register_node(peer.id, peer.km.public_key)
        n.km.register_node("client", client_km.public_key)
    for n in nodes: n.start()

    req = BFTMessage(MsgType.REQUEST, "client", 0, 0, "", "SafeVal")
    sig = client_km.sign(req.pack_for_signing())
    net.send("client", nodes[0].id, dataclasses.replace(req, signature=sig))
    
    await asyncio.sleep(5)
    
    honest_nodes = [n for n in nodes if n.behavior == ByzantineBehavior.HONEST]
    
    consistent = True
    decided_count = 0
    for n in honest_nodes:
        if 1 in n.executed_log:
            decided_count += 1
            if n.executed_log[1] != "SafeVal":
                consistent = False
                logger.error(f"Safety Violation! {n.id} decided wrong value: {n.executed_log[1]}")

    logger.info("-" * 50)
    if consistent and decided_count == 3:
        logger.info(f"SAFETY PASSED: 3/3 honest nodes committed correct value.")
    else:
        logger.error(f"SAFETY FAILED: Consistent={consistent}, Count={decided_count}/3")
    logger.info("-" * 50)

    for n in nodes: await n.stop()
    net._running = False
    return (consistent and decided_count >= 3), "Resilience to Liar"

async def demo_liveness():
    logger.info("="*50)
    logger.info("   DEMO: LIVENESS (Leader Crash & Recovery)")
    logger.info("="*50)
    
    node_ids = ["n0", "n1", "n2", "n3"]
    net = Network(seed=42)
    nodes = [
        BFTNode("n0", node_ids, net, ByzantineBehavior.CRASH_AFTER_SEQ_1),
        BFTNode("n1", node_ids, net, ByzantineBehavior.HONEST),
        BFTNode("n2", node_ids, net, ByzantineBehavior.HONEST),
        BFTNode("n3", node_ids, net, ByzantineBehavior.HONEST)
    ]
    client_km = KeyManager("client")
    for n in nodes:
        for peer in nodes: n.km.register_node(peer.id, peer.km.public_key)
        n.km.register_node("client", client_km.public_key)
    for n in nodes: n.start()

    req1 = BFTMessage(MsgType.REQUEST, "client", 0, 0, "", "Tx1")
    sig1 = client_km.sign(req1.pack_for_signing())
    net.send("client", nodes[0].id, dataclasses.replace(req1, signature=sig1))
    
    await asyncio.sleep(3)
    crash_time = time.time()
    logger.info(">>> SIMULATING LEADER CRASH (n0) ...")
    
    req2 = BFTMessage(MsgType.REQUEST, "client", 0, 0, "", "Tx2")
    sig2 = client_km.sign(req2.pack_for_signing())
    signed_req2 = dataclasses.replace(req2, signature=sig2)
    
    recovery_end = None
    
    while time.time() - crash_time < 15:
        net.send("client", nodes[1].id, signed_req2)
        
        await asyncio.sleep(0.1)
        
        if any(val == "Tx2" for val in nodes[1].executed_log.values()):
            recovery_end = time.time()
            break
            
    logger.info("-" * 50)
    if recovery_end:
        rec_time = recovery_end - crash_time
        logger.info(f"LIVENESS PASSED: System recovered in {rec_time:.2f}s.")
    else:
        logger.error(f"LIVENESS FAILED: System did not recover within timeout.")
    logger.info("-" * 50)

    for n in nodes: await n.stop()
    net._running = False
    
    return (recovery_end - crash_time) if recovery_end else 0, "Crash Recovery Time"

# Benchmark delle prestazioni con latenza e jitter configurabili
async def demo_benchmark_dynamic(num_nodes=4, num_decisions=10, base_latency: int = 20, jitter: int = 20):
    logger.info(f"=== DEMO: BENCHMARK N={num_nodes} (Decisions={num_decisions}) ===")
    node_ids = [f"n{i}" for i in range(num_nodes)]
    
    # Inizializzazione rete con i parametri di input
    net = Network(base_latency_ms=base_latency, jitter_ms=jitter, seed=100 + num_nodes)
    nodes = [BFTNode(nid, node_ids, net) for nid in node_ids]
    
    f = (num_nodes - 1) // 3
    client_km = KeyManager("client")
    for n in nodes:
        for p in nodes: n.km.register_node(p.id, p.km.public_key)
        n.km.register_node("client", client_km.public_key)
    for n in nodes: n.start()

    await asyncio.sleep(1)
    net.msg_count = 0
    start_time = time.time()
    
    for i in range(1, num_decisions + 1):
        req = BFTMessage(MsgType.REQUEST, "client", 0, 0, "", f"BenchVal-{i}")
        sig = client_km.sign(req.pack_for_signing())
        net.send("client", nodes[0].id, dataclasses.replace(req, signature=sig))
        
        while True:
            replies = sum(1 for n in nodes if i in n.executed_log)
            if replies >= f + 1: break
            await asyncio.sleep(0.01)
            
    elapsed = time.time() - start_time
    total_msgs = net.msg_count
    tps = num_decisions / elapsed if elapsed > 0 else 0
    
    for n in nodes: await n.stop()
    net._running = False
    
    logger.info(f"RESULT N={num_nodes} -> Time: {elapsed:.4f}s | Msgs: {total_msgs} | TPS: {tps:.2f}")
    return elapsed, total_msgs, tps

if __name__ == "__main__":
    asyncio.run(demo_benchmark_dynamic(5, 10, base_latency=15, jitter=5))
    asyncio.run(demo_safety())
    asyncio.run(demo_liveness())