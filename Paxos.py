from __future__ import annotations

import asyncio
import dataclasses
import enum
import heapq
import logging
import random
import time
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("paxos")

#Simulazione di un Ambiente di rete asincrono
class Network:
    def __init__(self, *, base_latency_ms: int = 20, jitter_ms: int = 20, drop_prob: float = 0.0, dup_prob: float = 0.0, seed: int = 42):
        self.base_latency_ms = base_latency_ms
        self.jitter_ms = jitter_ms
        self.drop_prob = drop_prob
        self.dup_prob = dup_prob
        self.inboxes: Dict[str, asyncio.Queue] = {}
        self._pq: List[Tuple[float, int, str, Any]] = [] #queue di priorità che ordina gli eventi in base al tempo di consegna simulato
        self._seq = 0
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._rand = random.Random(seed)
        self.seed_base = seed
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
    PREPARE = "PREPARE"
    PROMISE = "PROMISE"
    ACCEPT_REQ = "ACCEPT_REQ"
    ACCEPTED = "ACCEPTED"
    DECIDE = "DECIDE"

#Definisce il payload
@dataclasses.dataclass(frozen=True)
class Message:
    mtype: MsgType
    src: str
    dst: str
    n: int
    instance_id: int
    value: Optional[Any] = None
    last_accepted: Optional[Tuple[int, Any]] = None 

#Helper che genera numeri di proposta unici e crescenti usando epoch e index del nodo
#le epoch sono un contatore locale che il proposer incrementa ogni volta che tenta di avviare una nuova proposta
class ProposalNumber:
    MULT = 1_000_000 #per comodità di lettura degli n
    @staticmethod
    def make(epoch: int, node_index: int) -> int:
        return epoch * ProposalNumber.MULT + node_index

#Mantiene la stato persistente dell'Acceptor
class AcceptorState:
    def __init__(self):
        self.promised_n: int = -1
        self.accepted: Optional[Tuple[int, Any]] = None

#Rappresentazione generica di nodo
class PaxosNode:
    def __init__(self, node_id: str, all_nodes: List[str], network: Network, majority: int):
        self.id = node_id
        self.all_nodes = all_nodes
        self.network = network
        self.majority = majority
        self.inbox: asyncio.Queue = network.register(node_id)

        self.acceptor = AcceptorState()  #stato acceptor
        self.epoch = 0
        self.node_index = self.all_nodes.index(node_id) + 1  #importante per generare id unici per ogni proposta
        
        self.instance_id = 0
        self.decided_value: Optional[Any] = None
        self.wait_queue: Optional[asyncio.Queue] = None
        
        self._crashed = False #Flag per simulare il crash fisico del nodo

        try: nid_int = int(node_id[1:])
        except: nid_int = hash(node_id)
        self._rand = random.Random(network.seed_base + nid_int)

        self._task: Optional[asyncio.Task] = None
        self._stop = False

    def start(self):
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._stop = True
        if self._task: await self._task

    #Simula l'arresto improvviso del nodo (Crash)
    def crash(self):
        self._crashed = True
        logger.warning(f"{self.id} CRASHED e non risponde")

    #Helper per benchmark che resetta lo stato interno per permettere di riutilizzare gli stessi oggetti nodo
    #Se K = 1000, non ha senso distruggere ogni volta un nodo, semplicemente lo resettiamo e riusiamo
    def reset_consensus_state(self):
        self.instance_id += 1  #id incrementale
        self.decided_value = None
        self.acceptor = AcceptorState()
        self._crashed = False #Reset del crash per nuovi round di test

    #Cuore del nodo, preleva messaggi, scarta i vecchi e se il nodo è in attesa duplica il messaggio in una queue locale
    #Preleva i messaggi dalla inbox ed è relativa al singolo nodo
    async def _run(self):
        while not self._stop:
            try:
                msg: Message = await asyncio.wait_for(self.inbox.get(), timeout=0.1) #Single Consumer (solo _run può fare inbox.get())
                
                #Se il nodo e' crashato smette di processare messaggi dalla inbox
                if self._crashed:
                    continue

                if msg.instance_id != self.instance_id:
                    continue

                if self.wait_queue is not None:
                    self.wait_queue.put_nowait(msg)
                await self._handle(msg)
            except asyncio.TimeoutError:
                continue
    
    #Logica del Proposer
    async def propose(self, value: Any, *, timeout_s: float = 2.0, min_backoff_ms: int = 100, max_backoff_ms: int = 500):
        if self._crashed: return None #Un nodo morto non può avviare proposte
        if self.decided_value is not None: return self.decided_value
        
        current_inst = self.instance_id
        self.wait_queue = asyncio.Queue()  #Queue locale
        try:
            while not self._stop and self.decided_value is None and self.instance_id == current_inst and not self._crashed:
                self.epoch += 1 #contatore locale che il proposer incrementa ogni volta che tenta di avviare una nuova proposta
                n = ProposalNumber.make(self.epoch, self.node_index)
                logger.info(f"{self.id} PROPOSE start n={n} v={value} inst={self.instance_id}")

                for dst in self.all_nodes:
                    self._send(Message(MsgType.PREPARE, self.id, dst, n, self.instance_id))

                promises: Dict[str, Tuple[int, Any]] = {}
                highest_accepted = None
                deadline = time.time() + timeout_s
                
                #Fase 1
                while time.time() < deadline and len(promises) < self.majority and not self._crashed:
                    try:
                        remaining = deadline - time.time()
                        if remaining <= 0: break
                        msg = await asyncio.wait_for(self.wait_queue.get(), timeout=remaining) #attendi messaggio in queue locale
                        if msg.mtype == MsgType.PROMISE and msg.n == n: 
                            if msg.src not in promises: #src è la sorgente del messaggio
                                promises[msg.src] = msg.last_accepted
                                if msg.last_accepted: #cerchiamo di vedere se qualche acceptor ha accettato valori e di trovare quello con n maggiore
                                    if highest_accepted is None or msg.last_accepted[0] > highest_accepted[0]:
                                        highest_accepted = msg.last_accepted
                        if self.decided_value is not None: return self.decided_value
                    except asyncio.TimeoutError: break

                if self._crashed: break
                if len(promises) < self.majority:
                    delay = self._rand.uniform(min_backoff_ms, max_backoff_ms) / 1000.0
                    await asyncio.sleep(delay)
                    continue

                v = highest_accepted[1] if highest_accepted else value
                for dst in self.all_nodes:
                    self._send(Message(Message(MsgType.ACCEPT_REQ, self.id, dst, n, self.instance_id, value=v).mtype, self.id, dst, n, self.instance_id, value=v))

                accepts: set[str] = set()
                deadline = time.time() + timeout_s
                
                #Fase 2
                while time.time() < deadline and len(accepts) < self.majority and not self._crashed:
                    try:
                        remaining = deadline - time.time()
                        if remaining <= 0: break
                        msg = await asyncio.wait_for(self.wait_queue.get(), timeout=remaining)
                        if msg.mtype == MsgType.ACCEPTED and msg.n == n and msg.value == v:
                            accepts.add(msg.src)
                        if self.decided_value is not None: return self.decided_value
                    except asyncio.TimeoutError: break

                if self._crashed: break
                if len(accepts) >= self.majority:
                    await self._decide(v, announce=True)
                    return v
                else:
                    delay = self._rand.uniform(min_backoff_ms, max_backoff_ms) / 1000.0
                    await asyncio.sleep(delay)
                    continue
        finally:
            self.wait_queue = None
        return self.decided_value

    #Smista i messaggi ai metodi specifici
    async def _handle(self, msg: Message):
        if self._crashed: return
        if msg.mtype == MsgType.PREPARE: await self._on_prepare(msg)
        elif msg.mtype == MsgType.ACCEPT_REQ: await self._on_accept_req(msg)
        elif msg.mtype == MsgType.DECIDE:
            if self.decided_value is None: await self._decide(msg.value, announce=False)

    #Logica dell'acceptor fase 1 (Prepare)
    async def _on_prepare(self, msg: Message):
        if msg.n > self.acceptor.promised_n:
            self.acceptor.promised_n = msg.n
            reply = Message(MsgType.PROMISE, src=self.id, dst=msg.src, n=msg.n, instance_id=self.instance_id, last_accepted=self.acceptor.accepted)
            self._send(reply)

    #Logica dell'acceptor fase 2 (Accept)
    async def _on_accept_req(self, msg: Message):
        if msg.n >= self.acceptor.promised_n: #se arriva una proposta con n > la si accetta, non solo n =
            self.acceptor.promised_n = msg.n
            self.acceptor.accepted = (msg.n, msg.value)
            ack = Message(MsgType.ACCEPTED, src=self.id, dst=msg.src, n=msg.n, instance_id=self.instance_id, value=msg.value)
            self._send(ack)

    #Logica del Learner che, quando viene deciso un valore, fa il broadcast a tutti gli altri nodi
    async def _decide(self, value: Any, *, announce: bool):
        if self.decided_value is not None: return
        self.decided_value = value
        logger.info(f"{self.id} DECIDE {value} inst={self.instance_id}")
        if announce:
            for dst in self.all_nodes:
                if dst != self.id:
                    self._send(Message(MsgType.DECIDE, src=self.id, dst=dst, n=0, instance_id=self.instance_id, value=value))

    def _send(self, msg: Message):
        if not self._crashed:
            self.network.send(msg.src, msg.dst, msg)

async def demo(num_nodes: int = 5, num_decisions: int = 10, drop_prob: float = 0.05, dup_prob: float = 0.02, base_latency: int=0, jitter: int=0):
    node_ids = [f"n{i+1}" for i in range(num_nodes)]
    majority = num_nodes // 2 + 1
    net = Network(base_latency_ms=base_latency, jitter_ms=jitter, drop_prob=drop_prob, dup_prob=dup_prob, seed=42)
    net.start()

    nodes = [PaxosNode(nid, node_ids, net, majority) for nid in node_ids]
    for n in nodes: n.start()

    start_time = time.time()
    
    for i in range(num_decisions):
        for n in nodes: n.reset_consensus_state() #riutilizzo nodi
        
        target_node = nodes[i % num_nodes] #selezione a rotazione Round-Robin del proposer
        val = f"Val-{i+1}"
        
        await target_node.propose(val)
        
        await asyncio.sleep(0.01)

    end_time = time.time()
    elapsed = end_time - start_time
    
    tps = num_decisions / elapsed if elapsed > 0 else 0 #Transazioni al secondo
    total_msgs = net.msg_count

    for n in nodes: await n.stop()
    await net.stop()
    logger.info("\n")
    return elapsed, total_msgs, tps

#Verifica la natura CFT: il sistema deve funzionare con f crash su 2f+1 nodi
async def demo_cft(num_nodes: int = 5, num_crashes: int = 2):
    logger.info(f"=== TEST CFT: N={num_nodes}, Crashes={num_crashes} ===")
    node_ids = [f"n{i+1}" for i in range(num_nodes)]
    majority = num_nodes // 2 + 1
    net = Network(base_latency_ms=10, jitter_ms=5, seed=42)
    net.start()

    nodes = [PaxosNode(nid, node_ids, net, majority) for nid in node_ids]
    for n in nodes: n.start()

    #Crashiamo f nodi (minoranza)
    for i in range(num_nodes - 1, num_nodes - 1 - num_crashes, -1):
        nodes[i].crash()

    #Il Proposer n1 deve riuscire a raggiungere il consenso nonostante i crash
    val = "CFT-Value"
    logger.info(f"Invio proposta da {nodes[0].id} con {num_crashes} nodi crashati")
    result = await nodes[0].propose(val)

    #Verifica finale
    decided_count = sum(1 for n in nodes if n.decided_value == val)
    logger.info(f"Risultato: {result}. Nodi che hanno deciso: {decided_count}/{num_nodes - num_crashes} (vivi)")

    for n in nodes: await n.stop()
    await net.stop()
    return result == val

if __name__ == "__main__":
    try: 
        asyncio.run(demo())
        asyncio.run(demo_cft())
    except KeyboardInterrupt: pass