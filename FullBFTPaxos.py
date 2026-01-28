import asyncio
import dataclasses
import enum
import logging
import random
import time
import hashlib
import heapq
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes
from cryptography.exceptions import InvalidSignature

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("FullBFT")

#Gestisce la crittografia del nodo
class KeyManager:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self._private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048) #coppia di chiavi RSA
        self.public_key = self._private_key.public_key()
        self.pki = {}

    #Registra la chiave pubblica del nodo
    def register_node(self, node_id: str, public_key): self.pki[node_id] = public_key

    #Firma i messaggi garantendo non ripudiabilità
    def sign(self, message_bytes: bytes) -> bytes:
        return self._private_key.sign(message_bytes, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH), hashes.SHA256())
    
    #Verifica l'autenticità di un messaggio, se non è autentico viene scartato
    def verify(self, node_id: str, message_bytes: bytes, signature: bytes) -> bool:
        if node_id not in self.pki: return False
        try:
            self.pki[node_id].verify(signature, message_bytes, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH), hashes.SHA256())
            return True
        except InvalidSignature: return False

#Simulazione di un Ambiente di rete asincrono
class Network:
    def __init__(self, *, base_latency_ms: int = 20, jitter_ms: int = 20, drop_prob: float = 0.0, dup_prob: float = 0.0, seed: int = 42):
        self.base_latency_ms = base_latency_ms
        self.jitter_ms = jitter_ms
        self.drop_prob = drop_prob
        self.dup_prob = dup_prob
        self.inboxes: Dict[str, asyncio.Queue] = {} #queue di priorità che ordina gli eventi in base al tempo di consegna simulato
        self._pq: List[Tuple[float, int, str, Any]] = []
        self._seq = 0
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._rand = random.Random(seed)
        self.msg_count = 0 

    #Crea interfaccia di ricezione messaggi del nodo (inbox)
    def register(self, node_id: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self.inboxes[node_id] = q
        return q

    #Gestisce il ciclo di vita del task di background _run che processa i messaggi
    def start(self):
        if self._task is None:
            self._running = True
            self._task = asyncio.create_task(self._run())

    #Gestisce il ciclo di vita del task di background _run che processa i messaggi
    async def stop(self):
        self._running = False
        if self._task:
            await self._task
            self._task = None

    #Cuore della simulazione che smista i messaggi della Queue verso i destinatari (inbox singoli nodi)
    async def _run(self):
        while self._running:
            now = time.time()
            if self._pq and self._pq[0][0] <= now:
                _, _, dst, payload = heapq.heappop(self._pq)
                inbox = self.inboxes.get(dst)
                if inbox is not None:
                    await inbox.put(payload)
                continue
            sleep_for = 0.001
            if self._pq:
                sleep_for = max(0.0, min(0.05, self._pq[0][0] - now))
            await asyncio.sleep(sleep_for)

    #Calcola la latenza simulata e inserisce il messaggio nella Queue
    def _schedule(self, dst: str, payload: Any):
        latency = self.base_latency_ms + self._rand.randint(0, max(0, self.jitter_ms))
        deliver_at = time.time() + latency / 1000.0
        self._seq += 1
        heapq.heappush(self._pq, (deliver_at, self._seq, dst, payload))

    #Applica la logica di guasto prima di chiamare schedule() e quindi decide duplicazione e perdita messaggi
    def send(self, src: str, dst: str, payload: Any):
        self.msg_count += 1 
        if self._rand.random() < self.drop_prob:
            return
        self._schedule(dst, payload)
        if self._rand.random() < self.dup_prob:
            self._schedule(dst, payload)

#Definisce i messaggi del protocollo
class MsgType(enum.Enum):
    CLIENT_REQ = "CLIENT_REQ"
    PROPOSE = "PROPOSE"
    ACCEPT  = "ACCEPT"
    VIEW_CHANGE = "VIEW_CHANGE"
    NEW_VIEW = "NEW_VIEW"
    HEARTBEAT = "HEARTBEAT"

#Definisce il payload
@dataclasses.dataclass(frozen=True)
class BpMessage:
    mtype: MsgType
    src: str
    view: int
    seq: int
    val: Any = None      
    digest: str = ""  #hash256 del valore proposto usato per efficienza nel confronto dei voti
    signature: bytes = b"" #firma RSA mittente
    proof: Any = None #placeholder per future estensioni con certificati di quorum

    #Helper per impacchettamento di messaggi in byte per poi firmarli in RSA
    def pack_for_signing(self) -> bytes:
        content = f"{self.mtype.value}:{self.view}:{self.seq}:{self.digest}:{self.val}"
        return content.encode('utf-8')

#Definisce i possibili comportamenti del nodo anche quelli scorretti
class Behavior(enum.Enum):
    HONEST = "HONEST" #si comporta onestamente
    CRASH_AFTER_SEQ_1 = "CRASH_AFTER_SEQ_1" #partecipa alle sequenza 0 e poi crasha
    EQUIVOCATOR = "EQUIVOCATOR" #nodo bizantino che se leader fa attacchi di equivocation
    SILENT = "SILENT" #nodo che scarta tutti i messaggi che riceve riducendo il quorum

#Rappresentazione generica di un nodo
class FullBFTNode:
    def __init__(self, node_id: str, all_nodes: List[str], network: Network, behavior: Behavior = Behavior.HONEST):
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
        self.log = {}        
        self.val_cache = {}  
        
        self.accepts = defaultdict(set)
        
        self.received_proposals = set()
        self.vc_votes = defaultdict(set)
        self.in_view_change = False
        self.last_msg_time = time.time()
        self.timeout_interval = 3.0 

        self._stop = False
        self._tasks = []
        self._crashed = False

    #Fornisce il leader corrente (Round Robin quindi ciclico e deterministico)
    def get_primary(self, view):
        return self.all_nodes[view % self.N]

    #Fa partire le task in background
    def start(self):
        self._tasks.append(asyncio.create_task(self._run()))
        self._tasks.append(asyncio.create_task(self._monitor_liveness()))
        self._tasks.append(asyncio.create_task(self._heartbeat_loop()))

    #Gestione stop del nodo
    async def stop(self):
        self._stop = True
        for t in self._tasks: t.cancel()

    #Fa l'hash256 di un valore
    def _hash(self, val: Any) -> str:
        return hashlib.sha256(str(val).encode()).hexdigest()

    #Firma, impacchetta e manda il messaggio firmato sulla rete
    def broadcast(self, msg: BpMessage):
        sig = self.km.sign(msg.pack_for_signing())
        signed = dataclasses.replace(msg, signature=sig)
        for dst in self.all_nodes:
            self.network.send(self.id, dst, signed)

    #Cuore del nodo che preleva messaggi dalla inbox,li dà ad handle e gestisce la Behavior del nodo
    async def _run(self):
        while not self._stop:
            try:
                msg = await self.inbox.get()
                if self.behavior == Behavior.CRASH_AFTER_SEQ_1 and self.seq >= 1:
                    if not self._crashed:
                        logger.critical(f"{self.id} CRASHING now (simulated)!")
                        self._crashed = True
                    continue 
                if self.behavior == Behavior.SILENT: continue
                await self._handle(msg)
            except asyncio.CancelledError: break

    #Monitora se il leader è crashato
    async def _monitor_liveness(self):
        while not self._stop:
            await asyncio.sleep(0.1)
            if self._crashed: break
            
            if self.get_primary(self.view) != self.id and not self.in_view_change: #verifichiamo anche se stiamo facendo view change
                if time.time() - self.last_msg_time > self.timeout_interval:
                    logger.warning(f" {self.id} TIMEOUT on View {self.view}. Leader {self.get_primary(self.view)} silent.")
                    await self._start_view_change()

    #Task in brackground usato solo dal leader per inviare ai follower i messaggi heartbeat
    async def _heartbeat_loop(self):
        while not self._stop:
            if self._crashed: break
            if self.get_primary(self.view) == self.id and not self.in_view_change:
                self.broadcast(BpMessage(MsgType.HEARTBEAT, self.id, self.view, 0))
            await asyncio.sleep(0.5)

    #Smista i messaggi alle specifiche funzioni dopo averli verificati se serve
    async def _handle(self, msg: BpMessage):
        if msg.mtype != MsgType.CLIENT_REQ:
            if not self.km.verify(msg.src, msg.pack_for_signing(), msg.signature): return
        
        if msg.mtype != MsgType.CLIENT_REQ:
            if msg.view < self.view and msg.mtype != MsgType.VIEW_CHANGE: return

        if msg.src == self.get_primary(self.view) and msg.view == self.view:
            self.last_msg_time = time.time()

        if msg.mtype == MsgType.VIEW_CHANGE: await self._handle_view_change(msg)
        elif msg.mtype == MsgType.NEW_VIEW: await self._handle_new_view(msg)
        
        if self.in_view_change: return #Se stiamo cambiando leader allora blocca qualunque messaggio non riguardi la view change

        if msg.mtype == MsgType.CLIENT_REQ: await self._handle_client_req(msg)
        elif msg.mtype == MsgType.PROPOSE: await self._handle_propose(msg)
        elif msg.mtype == MsgType.ACCEPT: await self._handle_accept(msg)

    #Gestisce le richieste dei client e solo il Leader può avviare il protocollo per una richiesta esterna
    #Assegna un numero di sequenza (seq) univoco e avvia la fase di PROPOSE
    #Include la logica Equivocator per simulare un leader bizantino che invia valori diversi (Split Brain)
    async def _handle_client_req(self, msg: BpMessage):
        if self.id != self.get_primary(self.view): return
        
        self.seq += 1
        digest = self._hash(msg.val)
        self.val_cache[digest] = msg.val 
        
        if self.behavior == Behavior.EQUIVOCATOR:
            logger.warning(f"{self.id} EQUIVOCATING! Sending 'Blue' to half, 'Red' to half.")
            
            msgA = BpMessage(MsgType.PROPOSE, self.id, self.view, self.seq, val="Blue")
            sigA = self.km.sign(msgA.pack_for_signing())
            signedA = dataclasses.replace(msgA, signature=sigA)
            
            msgB = BpMessage(MsgType.PROPOSE, self.id, self.view, self.seq, val="Red")
            sigB = self.km.sign(msgB.pack_for_signing())
            signedB = dataclasses.replace(msgB, signature=sigB)
            
            mid = len(self.all_nodes) // 2
            for i, dst in enumerate(self.all_nodes):
                pkt = signedA if i < mid else signedB
                self.network.send(self.id, dst, pkt)
            return

        #Comportamento onesto con broadcast della PROPOSE a tutti
        logger.info(f"{self.id} [LEADER v{self.view}] PROPOSE seq={self.seq} v={msg.val}")
        self.broadcast(BpMessage(MsgType.PROPOSE, self.id, self.view, self.seq, val=msg.val))

    #Gestisce la ricezione di una PROPOSE dal Leader (Fase 1)
    #I nodi verificano che il messaggio provenga effettivamente dal Leader della view corrente
    async def _handle_propose(self, msg: BpMessage):
        if msg.src != self.get_primary(self.view): return
        key = (msg.view, msg.seq)
        
        if key in self.received_proposals: return
        self.received_proposals.add(key)
        
        digest = self._hash(msg.val)
        self.val_cache[digest] = msg.val
        
        self.broadcast(BpMessage(MsgType.ACCEPT, self.id, self.view, msg.seq, digest=digest))

    #Gestisce la ricezione dei voti ACCEPT (Fase 2)
    #Grazie alle firme RSA (non ripudiabilità), non serve una terza fase di COMMIT come in PBFT
    async def _handle_accept(self, msg: BpMessage):
        if msg.view != self.view: return
        if msg.seq in self.log: return 
        
        key = (msg.view, msg.seq, msg.digest)
        self.accepts[key].add(msg.src)
        
        votes = len(self.accepts[key])
        
        if votes >= self.quorum:
            val = self.val_cache.get(msg.digest, "UNKNOWN")
            if val != "UNKNOWN":
                self.log[msg.seq] = val
                logger.info(f"{self.id} DECIDED seq={msg.seq} val={val} (View {self.view})")
                self.last_msg_time = time.time()

    #Avvia la procedura di view change quando scatta il timeout degli heartbeat del leader
    #Il nodo entra in modalità 'in_view_change', smette di accettare nuove proposte normali
    #e invia il suo voto per passare alla view successiva
    async def _start_view_change(self):
        self.in_view_change = True
        next_view = self.view + 1
        logger.info(f"{self.id} START VIEW CHANGE -> v{next_view}")
        self.broadcast(BpMessage(MsgType.VIEW_CHANGE, self.id, next_view, 0))
        self.vc_votes[next_view].add(self.id)

    #Gestisce i voti per il cambio di View, se si ottiene il quorum di voti VIEW_CHANGE
    #allora cambia il leader con NEW_VIEW
    async def _handle_view_change(self, msg: BpMessage):
        if msg.view <= self.view: return
        self.vc_votes[msg.view].add(msg.src)
        
        if self.get_primary(msg.view) == self.id:
            count = len(self.vc_votes[msg.view])
            if count >= self.quorum:
                if self.view < msg.view: 
                    logger.info(f"{self.id} NEW LEADER for v{msg.view} (Votes: {count})")
                    self.view = msg.view
                    self.in_view_change = False
                    self.last_msg_time = time.time()
                    self.broadcast(BpMessage(MsgType.NEW_VIEW, self.id, self.view, 0))

    #Gestisce l'installazione della nuova view
    #I nodi follower ricevono la prova che il cambio view è avvenuto con successo
    #Aggiornano il loro contatore di view, escono dallo stato in_view_change e riprendono le operazioni
    async def _handle_new_view(self, msg: BpMessage):
        if msg.src != self.get_primary(msg.view): return
        if msg.view <= self.view: return
        
        logger.info(f"{self.id} INSTALLED NEW VIEW v{msg.view} from {msg.src}")
        self.view = msg.view
        self.in_view_change = False
        self.last_msg_time = time.time()

#Simula un client che invia K richieste sequenziali e attende che la rete prenda decisioni
async def demo_benchmark_full(num_nodes=4, num_decisions=10, base_latency: int=0, jitter: int=0):
    logger.info(f"=== FULL AUTH PAXOS BENCHMARK (N={num_nodes}, K={num_decisions}) ===")
    node_ids = [f"n{i}" for i in range(num_nodes)]

    net = Network(base_latency_ms=base_latency, jitter_ms=jitter, seed=123)
    net.start()
    
    nodes = [FullBFTNode(nid, node_ids, net) for nid in node_ids]
    
    f = (num_nodes - 1) // 3
    for n in nodes:
        for p in nodes: n.km.register_node(p.id, p.km.public_key)
    for n in nodes: n.start()
    
    start_time = time.time()
    for i in range(1, num_decisions + 1):
        req = BpMessage(MsgType.CLIENT_REQ, "client", 0, 0, val=f"Tx-{i}")
        net.send("client", nodes[0].id, req)
        
        while True:
            decided = sum(1 for n in nodes if i in n.log)
            if decided >= f + 1: break
            await asyncio.sleep(0.01)
            
    elapsed = time.time() - start_time
    logger.info(f"RESULT -> Time: {elapsed:.2f}s | Msgs: {net.msg_count}")
    
    for n in nodes: await n.stop()
    await net.stop()
    return elapsed, net.msg_count, num_decisions/elapsed

#Test Liveness: verifica la capacità di recupero dopo il crash del Leader
#1. Invia Tx1
#2. Simula Crash del Leader n0
#3. Invia Tx2 ed il sistema deve andare in timeout, eleggere n1 e processare Tx2
async def demo_liveness_full():
    logger.info("\n=== FULL LIVENESS CHECK (Leader Crash) ===")
    node_ids = ["n0", "n1", "n2", "n3"]

    net = Network(base_latency_ms=15, jitter_ms=5, seed=42)
    net.start()
    
    nodes = [
        FullBFTNode("n0", node_ids, net, Behavior.CRASH_AFTER_SEQ_1),
        FullBFTNode("n1", node_ids, net, Behavior.HONEST),
        FullBFTNode("n2", node_ids, net, Behavior.HONEST),
        FullBFTNode("n3", node_ids, net, Behavior.HONEST)
    ]
    
    for n in nodes:
        for p in nodes: n.km.register_node(p.id, p.km.public_key)
    for n in nodes: n.start()

    logger.info(">>> Tx1 (Normal)")
    req1 = BpMessage(MsgType.CLIENT_REQ, "client", 0, 0, val="Tx1")
    net.send("client", "n0", req1)
    await asyncio.sleep(2)
    
    logger.info(">>> Leader n0 should be dead. Sending Tx2...")
    recovery_end = None
    start_wait = time.time()
    req2 = BpMessage(MsgType.CLIENT_REQ, "client", 0, 0, val="Tx2")
    
    while time.time() - start_wait < 10.0:
        for nid in ["n1", "n2", "n3"]: net.send("client", nid, req2)
        await asyncio.sleep(0.5)
        if any(n.log.get(2) == "Tx2" for n in nodes[1:]):
            recovery_end = time.time()
            break
            
    for n in nodes: await n.stop()
    await net.stop()
    
    if recovery_end:
        t = recovery_end - start_wait
        logger.info(f"FULL LIVENESS PASSED: Recovered in {t:.2f}s")
        return t, "RECOVERED"
    else:
        logger.error("LIVENESS FAILED.")
        return 0, "FAILED"

#Test Safety: verifica la resistenza contro un leader bizantino
#Il leader invia valori diversi ("Blue" e "Red"). Il protocollo deve bloccarsi
#piuttosto che violare la consistenza (Split Brain)
async def demo_safety():
    logger.info("\n=== FULL SAFETY CHECK (Leader Equivocation) ===")
    node_ids = ["n0", "n1", "n2", "n3"]

    net = Network(base_latency_ms=15, jitter_ms=5, seed=999)
    net.start()
    
    nodes = [
        FullBFTNode("n0", node_ids, net, Behavior.EQUIVOCATOR),
        FullBFTNode("n1", node_ids, net),
        FullBFTNode("n2", node_ids, net),
        FullBFTNode("n3", node_ids, net)
    ]
    
    for n in nodes:
        for p in nodes: n.km.register_node(p.id, p.km.public_key)
    for n in nodes: n.start()
    
    req = BpMessage(MsgType.CLIENT_REQ, "client", 0, 0, val="Trigger")
    net.send("client", "n0", req)
    
    await asyncio.sleep(3)
    
    decisions = defaultdict(int)
    for n in nodes[1:]:
        if 1 in n.log:
            decisions[n.log[1]] += 1
            
    for n in nodes: await n.stop()
    await net.stop()

    if len(decisions) == 0:
        logger.info("SAFETY PASSED: System Stalled (No Split Brain).")
        return True, "PASSED (Stalled)"
    elif len(decisions) == 1:
        logger.info(f"SAFETY PASSED: Converged to single value {list(decisions.keys())[0]}")
        return True, "PASSED (Converged)"
    else:
        logger.error(f"SAFETY FAILED: Split Brain {decisions}")
        return False, "FAILED (Split)"

if __name__ == "__main__":
    try:
        asyncio.run(demo_benchmark_full(4, 10))
        asyncio.run(demo_liveness_full())
        asyncio.run(demo_safety())
    except KeyboardInterrupt: pass