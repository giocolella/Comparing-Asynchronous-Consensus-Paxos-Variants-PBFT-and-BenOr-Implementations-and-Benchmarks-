from __future__ import annotations
import asyncio
import dataclasses
import enum
import heapq
import logging
import random
import time
from collections import defaultdict, Counter
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("multipaxos")

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

    #Helper che esegue il broadcast di un payload ad una lista di nodi destinatari
    def broadcast(self, src: str, dsts: List[str], payload: Any):
        for d in dsts:
            self.send(src, d, payload)

#Definisce i messaggi del protocollo
class MsgType(enum.Enum):
    PREPARE = "PREPARE"
    PROMISE = "PROMISE"
    ACCEPT_REQ = "ACCEPT_REQ"
    ACCEPTED = "ACCEPTED"
    DECIDE = "DECIDE"  #messaggio inviato dal leader a tutti i nodi per comunicare il valore deciso
    HEARTBEAT = "HEARTBEAT"
    ELECTION = "ELECTION"
    ELECTION_OK = "ELECTION_OK" #messaggio mandato da leader "più forte" ad altri aspiranti leader per farli desistere ed adeguare (durante elezione)
    COORDINATOR = "COORDINATOR" #messaggio che il leader manda a tutti per dire di essere stato eletto (fine elezione)
    CLIENT_REQ = "CLIENT_REQ" #se il client contatta un nodo follower, esso fa forwarding del messaggio al leader
    CLIENT_RESP = "CLIENT_RESP" #inviato dal leader alla source di CLIENT_REQ per sbloccare il client e dargli il valore deciso

#Definisce il payload
@dataclasses.dataclass(frozen=True)
class Message:
    mtype: MsgType
    src: str
    dst: str
    n: int
    slot: int #MP prende diverse decisioni indipendenti, uno slot indica una specifica decizione
    value: Optional[Any] = None
    last_accepted: Optional[Tuple[int, Any]] = None 
    view: Optional[int] = None  #rappresenta il mandato del leader corrente, se view = 6 allora scarto ordini con view = 5

#Helper che genera numeri di proposta unici e crescenti usando epoch e index del nodo
#le epoch sono un contatore locale che il proposer incrementa ogni volta che tenta di avviare una nuova proposta
class ProposalNumber:
    MULT = 1_000_000 #per comodità di lettura degli n
    @staticmethod
    def make(epoch: int, node_index: int) -> int:
        return epoch * ProposalNumber.MULT + node_index #sarebbe promised_n_global

#Stato singola decisione indipendente (simile a Basic Paxos AcceptorState)
@dataclasses.dataclass
class SlotState:
    promised_n: int = -1
    accepted: Optional[Tuple[int, Any]] = None 

#Mantiene la stato persistente dell'Acceptor nella visione globale a leader "fisso"
class AcceptorState:
    def __init__(self):
        self.slots: Dict[int, SlotState] = defaultdict(SlotState)
        self.promised_n_global: int = -1  #fondamentale per MP: numero con cui il leader corrente comanda. Evita la fase 1

# Rappresentazione generica di nodo che può essere follower, candidate o leader
class MPaxosNode:
    def __init__(self, node_id: str, all_nodes: List[str], network: Network, majority: int):
        self.id = node_id
        self.all_nodes = sorted(all_nodes, key=lambda x: int(x[1:])) 
        self.network = network
        self.majority = majority
        self.inbox: asyncio.Queue = network.register(node_id)

        self.view = 0
        self.epoch_local = 0
        self.node_index = self.all_nodes.index(node_id) + 1
        self.current_n: Optional[int] = None
        self.fast_path: bool = False  

        self.acceptor = AcceptorState()
        self.accept_counts: Counter[Tuple[int, int, Any]] = Counter()
        self._seen_accepted: Dict[Tuple[int, int, Any], set[str]] = defaultdict(set)
        self.log: Dict[int, Any] = {}
        self.commit_index: int = 0
        self.accepted_seen: Dict[int, Any] = {}
        self.proposed_in_view: Dict[int, Any] = {}

        self.leader_id: Optional[str] = None
        self._last_hb: float = 0.0
        self._hb_interval: float = 0.2
        self._election_timeout_base: float = 5.0
        self._election_in_progress: bool = False
        self._rand = random.Random(1234 + self.node_index)

        self.wait_queue: Optional[asyncio.Queue] = None
        self.proposal_lock = asyncio.Lock() 

        self._crashed = False # Flag per simulare il crash fisico del nodo
        self._stop = False
        self._task: Optional[asyncio.Task] = None
        self._hb_task: Optional[asyncio.Task] = None

    def start(self):
        self._task = asyncio.create_task(self._run())
        self._hb_task = asyncio.create_task(self._heartbeats())

    async def stop(self):
        self._stop = True
        if self._task: await self._task
        if self._hb_task: await self._hb_task

    # Simula l'arresto improvviso del nodo (Crash)
    def crash(self):
        self._crashed = True
        logger.warning(f"{self.id} CRASHED: smette di rispondere e inviare heartbeat")

    # Cuore del nodo, preleva messaggi dalla inbox locale
    async def _run(self):
        await asyncio.sleep(self._rand.uniform(0.0, 0.3)) 
        while not self._stop:
            # Se il nodo e' crashato, rimane in un loop silente
            if self._crashed:
                await asyncio.sleep(0.1)
                continue

            if self.leader_id != self.id:
                if (self.leader_id is None) or (time.time() - self._last_hb > self._calc_timeout()):
                    await self._start_view_change_logic() # Rinominata per chiarezza
            
            try:
                msg: Message = await asyncio.wait_for(self.inbox.get(), timeout=0.1)
                if self.wait_queue is not None:
                    self.wait_queue.put_nowait(msg)
                await self._handle(msg)
            except asyncio.TimeoutError:
                continue

    # Logica di elezione (Variante Bully)
    async def _start_view_change_logic(self):
        if self._election_in_progress or self._crashed: return
        self._election_in_progress = True
        try:
            self.view += 1
            higher = [nid for nid in self.all_nodes if int(nid[1:]) > int(self.id[1:])]
            if not higher:
                await self._become_leader()
                return
            for dst in higher:
                self._send(Message(MsgType.ELECTION, src=self.id, dst=dst, n=-1, slot=0, view=self.view))
            
            deadline = time.time() + 1.0 # Timeout piu' breve per l'elezione
            while time.time() < deadline:
                if self._crashed: return
                try:
                    msg = await asyncio.wait_for(self.inbox.get(), timeout=0.1)
                    if msg.mtype == MsgType.ELECTION_OK: return # Un nodo superiore ha preso il comando
                    await self._handle(msg)
                except asyncio.TimeoutError: continue
            await self._become_leader()
        finally: self._election_in_progress = False
    
    #Interfaccia per richieste del client che vorrebbe in value deciso in un certo slot
    async def submit(self, slot: int, value: Any) -> Any:
        while slot not in self.log: #finchè lo slot richiesto dal client non è iniziato
            if self.leader_id and self.leader_id != self.id: #se il follower riceve la richiesta la manda al leader
                self._send(Message(MsgType.CLIENT_REQ, src=self.id, dst=self.leader_id, n=-1, slot=slot, value=value, view=self.view))
                deadline = time.time() + 2.0
                while time.time() < deadline:
                    if slot in self.log: return self.log[slot]
                    await asyncio.sleep(0.01)
            elif self.leader_id == self.id: #se il leader riceve la richiesta fa partire il consenso
                await self._propose_for_slot(slot, value)
                if slot not in self.log: await asyncio.sleep(0.01) 
            else: #se non c'è leader si aspetta
                await asyncio.sleep(0.1)
        return self.log[slot] #potrebbe essere diverso da quello richiesto dal client

    #Effettivo generatore di proposte N
    def _fresh_n(self) -> int:
        self.epoch_local += 1
        return ProposalNumber.make(self.view * 10_000 + self.epoch_local, self.node_index)

    #Variante Bully: un nodo accetta come leader solo qualcuno con un ID maggiore del proprio
    #Il leader esegue la fase 1 per ottenere fast path
    async def _establish_leadership(self) -> bool:
        async with self.proposal_lock: #il leader potrebbe ricevere richieste dai client anche durante l'elezione, serve gestione concorrenza
            n = self._fresh_n()
            self.current_n = n
            logger.info(f"{self.id} [LEADER] establishing global promise n={n} view={self.view}")
            for dst in self.all_nodes:
                self._send(Message(MsgType.PREPARE, self.id, dst, n=n, slot=0, view=self.view))
            
            promises: set[str] = set()
            deadline = time.time() + 5.0
            self.wait_queue = asyncio.Queue()
            try:
                while time.time() < deadline and len(promises) < self.majority:
                    try:
                        remaining = deadline - time.time()
                        if remaining <= 0: break
                        msg = await asyncio.wait_for(self.wait_queue.get(), timeout=remaining)
                        if msg.mtype == MsgType.PROMISE and msg.n == n and msg.slot == 0: #slot 0 è riservato a messaggi di elezione del leader
                            promises.add(msg.src)
                    except asyncio.TimeoutError: continue
            finally:
                self.wait_queue = None

            ok = len(promises) >= self.majority
            self.fast_path = ok
            if ok: logger.info(f"{self.id} [LEADER] fast path ENABLED n={n}")
            return ok

    #Logica del consenso
    async def _propose_for_slot(self, slot: int, value: Any) -> Any:
        if self.leader_id and self.leader_id != self.id: return None
        async with self.proposal_lock: #il leader potrebbe ricevere tante richieste da più client, serve gestione concorrenza
            if slot in self.log: return self.log[slot] #se uno slot è nel log allora è già stato deciso, non serve altro
            if slot in self.proposed_in_view: #memoria a breve termine del leader per evitare che richieste di client contaminino la sua decisione per lo slot corrente
                value = self.proposed_in_view[slot]
            else:
                self.proposed_in_view[slot] = value

            max_seen = max(self.accepted_seen.keys(), default=0) #è lo slot più alto visto dal nodo,accepted_seen è locale e contiene tutti gli slot per cui il nodo ha visto messaggi ACCEPTED 
            if self.fast_path and self.current_n is not None and slot > max_seen:
                n = self.current_n
                logger.info(f"{self.id} [FAST] slot={slot} n={n} v={value}")
                for dst in self.all_nodes:
                    self._send(Message(MsgType.ACCEPT_REQ, self.id, dst, n=n, slot=slot, value=value, view=self.view))
                if await self._wait_for_quorum(n, slot, value): return self.log[slot]

            #fast path non attivo, procediamo con fase 1
            n = self._fresh_n()
            self.current_n = n
            logger.info(f"{self.id} [LEADER] PROPOSE slot={slot} n={n} v={value}")
            for dst in self.all_nodes:
                self._send(Message(MsgType.PREPARE, self.id, dst, n=n, slot=slot, view=self.view))
            
            collected_promises = []
            deadline = time.time() + 5.0
            self.wait_queue = asyncio.Queue()
            try: 
                while time.time() < deadline and len(collected_promises) < self.majority:
                    try:
                        msg = await asyncio.wait_for(self.wait_queue.get(), timeout=0.2)
                        if msg.mtype == MsgType.PROMISE and msg.n == n and msg.slot == slot:
                            collected_promises.append(msg)
                    except: pass
            finally: self.wait_queue = None
            
            if len(collected_promises) < self.majority: return None
            
            highest_n = -1
            adopted_val = None
            for p in collected_promises:
                if p.last_accepted:
                    pn, pval = p.last_accepted
                    if pn > highest_n:
                        highest_n = pn
                        adopted_val = pval
            
            final_value = adopted_val if adopted_val is not None else value
            if adopted_val is not None:
                logger.warning(f"{self.id} RECOVERING slot={slot}: Replacing {value} with adopted {final_value}")

            #fase 2
            for dst in self.all_nodes:
                self._send(Message(MsgType.ACCEPT_REQ, self.id, dst, n=n, slot=slot, value=final_value, view=self.view))
            
            if await self._wait_for_quorum(n, slot, final_value): return self.log[slot]
            return None

    #Attende di ricevere una maggioranza di ACCEPT su uno slot, con un valore ed un n
    async def _wait_for_quorum(self, n, slot, value):
        accepts = 0
        deadline = time.time() + 5.0
        self.wait_queue = asyncio.Queue()
        try:
            while time.time() < deadline and accepts < self.majority:
                if slot in self.log: return True
                try:
                    msg = await asyncio.wait_for(self.wait_queue.get(), timeout=0.2)
                    if msg.mtype == MsgType.ACCEPTED and msg.n == n and msg.slot == slot and msg.value == value:
                        accepts += 1
                except: pass
        finally: self.wait_queue = None
        
        if accepts >= self.majority:
            await self._decide(slot, value, announce=True, n=n)
            return True
        return False

    #Smista i messaggi ricevuti da _run alle specifiche funzioni
    async def _handle(self, msg: Message):
        if msg.mtype == MsgType.PREPARE: await self._on_prepare(msg)
        elif msg.mtype == MsgType.ACCEPT_REQ: await self._on_accept_req(msg)
        elif msg.mtype == MsgType.ACCEPTED: await self._on_accepted(msg)
        elif msg.mtype == MsgType.DECIDE:
            if msg.slot not in self.log: await self._decide(msg.slot, msg.value, announce=False, n=msg.n)
        elif msg.mtype == MsgType.HEARTBEAT: await self._on_heartbeat(msg)
        elif msg.mtype == MsgType.ELECTION: await self._on_election(msg)
        elif msg.mtype == MsgType.COORDINATOR: await self._on_coordinator(msg)
        elif msg.mtype == MsgType.CLIENT_REQ: asyncio.create_task(self._handle_client_req_task(msg))
        elif msg.mtype == MsgType.CLIENT_RESP:
            pass

    #Relativo alla fase 1 dell'acceptor
    async def _on_prepare(self, msg: Message):
        if msg.slot == 0: #elezione in corso
            if msg.n > self.acceptor.promised_n_global: #controlla se hai già fatto una promessa più alta ad altro leader
                self.acceptor.promised_n_global = msg.n
                reply = Message(MsgType.PROMISE, src=self.id, dst=msg.src, n=msg.n, slot=0, view=msg.view)
                self._send(reply)
            return
        st = self.acceptor.slots[msg.slot] #SlotState specifico per msg.slot
        if msg.n > max(st.promised_n, self.acceptor.promised_n_global): #regola fondamentale
            st.promised_n = msg.n
            reply = Message(MsgType.PROMISE, src=self.id, dst=msg.src, n=msg.n, slot=msg.slot, last_accepted=st.accepted, view=msg.view)
            self._send(reply)

    #Relativo alla fase 2 dell'acceptor
    async def _on_accept_req(self, msg: Message):
        st = self.acceptor.slots[msg.slot]
        if msg.n >= max(st.promised_n, self.acceptor.promised_n_global):
            st.promised_n = max(st.promised_n, msg.n)
            st.accepted = (msg.n, msg.value)
            ack = Message(MsgType.ACCEPTED, src=self.id, dst=msg.src, n=msg.n, slot=msg.slot, value=msg.value, view=msg.view)
            self._send(ack)

    #Logica del Learner che smista i messaggi di accettazione per gli slot e nel caso ci sia maggioranza chiama _decide
    #Relativo a messaggi di tipo ACCEPTED
    async def _on_accepted(self, msg: Message):
        self.accepted_seen[msg.slot] = self.accepted_seen.get(msg.slot, msg.value)
        key = (msg.slot, msg.n, msg.value)
        self._seen_accepted[key].add(msg.src)
        if msg.slot not in self.log and len(self._seen_accepted[key]) >= self.majority:
            await self._decide(msg.slot, msg.value, announce=True, n=msg.n)

    async def _decide(self, slot: int, value: Any, *, announce: bool, n: int):
        if slot in self.log: return
        self.log[slot] = value
        logger.info(f"{self.id} DECIDE slot={slot} v={value}")
        while self.commit_index + 1 in self.log: self.commit_index += 1 #il log deve essere ordinato, se sono allo slot 3 ma lo slot 2 non è ancora stato deciso allora aspetto
        if announce: #se richiesto fa il broadcast di messaggi DECIDE per allineare i follower al valore deciso
            for dst in self.all_nodes:
                self._send(Message(MsgType.DECIDE, src=self.id, dst=dst, n=n, slot=slot, value=value, view=self.view))

    #Calcola quanto tempo aspettare prima di dichiarare il Leader "morto" e avviare una nuova elezione
    #inoltre ha elementi random per evitare che tutti si accorgano allo stesso momento che il leader è morto
    #evita che tutti si candidino allo stesso momento
    def _calc_timeout(self) -> float: return self._election_timeout_base + self._rand.random() * 0.5

    #Task in background eseguito solo dal leader, ogni tanto invia messaggi heartbeat ai follower
    async def _heartbeats(self):
        while not self._stop:
            if self.leader_id == self.id:
                for dst in self.all_nodes:
                    if dst != self.id: self._send(Message(MsgType.HEARTBEAT, src=self.id, dst=dst, n=-1, slot=0, view=self.view))
            await asyncio.sleep(self._hb_interval)

    #Task in background eseguito solo dai follower, se riceve heartbeat resetta il timer perchè il leader è vivo
    #Controlla inoltre se riceve una view maggiore
    async def _on_heartbeat(self, msg: Message):
        self._last_hb = time.time() #reset timer che è controllato da _run
        if (self.leader_id != msg.src) or (msg.view and msg.view >= self.view): #controlla se il messaggio appartiene ad una view maggiore
            self.leader_id = msg.src
            if msg.view is not None: self.view = max(self.view, msg.view) #si adegua alla nuova view, se necessario
            if self.leader_id != self.id: self.fast_path = False #Accetta qualcun altro come leader quindi esso non ha più fast path

    #Avviata quando scade il timer degli heartbeat, chi se ne accorge manda ELECTION a nodi con ID superiore
    #se non rispondono allora si proclama lui leader
    async def _start_election(self):
        if self._election_in_progress: return
        self._election_in_progress = True
        try:
            self.view += 1
            higher = [nid for nid in self.all_nodes if int(nid[1:]) > int(self.id[1:])] #cerca tutti i nodi con id più alto
            if not higher:
                await self._become_leader()
                return
            for dst in higher:
                self._send(Message(MsgType.ELECTION, src=self.id, dst=dst, n=-1, slot=0, view=self.view))
            deadline = time.time() + self._calc_timeout()
            while time.time() < deadline:
                try:
                    msg = await asyncio.wait_for(self.inbox.get(), timeout=0.1)
                    if msg.mtype == MsgType.ELECTION_OK and msg.src in higher: return
                    await self._handle(msg)
                except asyncio.TimeoutError: continue
            await self._become_leader()
        finally: self._election_in_progress = False

    #Funzione relativa al messaggio ELECTION
    async def _on_election(self, msg: Message):
        if int(self.id[1:]) > int(msg.src[1:]):
            self._send(Message(MsgType.ELECTION_OK, src=self.id, dst=msg.src, n=-1, slot=0, view=max(self.view, msg.view or 0)))
            if self.leader_id != self.id: await self._start_election()

    #Il leader appena eletto fa le dovute modifiche dopo l'elezione ed avvia la fase 1
    async def _become_leader(self):
        self.leader_id = self.id
        self._last_hb = time.time()
        self.fast_path = False 
        self.proposed_in_view.clear()
        logger.info(f"{self.id} BECOMES LEADER view={self.view}")
        for dst in self.all_nodes:
            if dst != self.id: self._send(Message(MsgType.COORDINATOR, src=self.id, dst=dst, n=-1, slot=0, view=self.view))
        asyncio.create_task(self._establish_leadership()) #task lanciata in background

    #Relativo al messaggio COORDINATOR
    async def _on_coordinator(self, msg: Message):
        if msg.view is not None and msg.view >= self.view:
            self.view = msg.view
            self.leader_id = msg.src
            self._last_hb = time.time()
            self.fast_path = False
            logger.info(f"{self.id} accepts coordinator {self.leader_id} view={self.view}")

    #Logica del Client disaccoppiata da quella del consenso che permette al Leader di processare richieste in batch
    #senza bloccare il thread di rete in attesa che il client risponda
    async def _handle_client_req_task(self, msg: Message):
        decided = await self._propose_for_slot(msg.slot, msg.value)
        if decided is not None:
            self._send(Message(MsgType.DECIDE, src=self.id, dst=msg.src, n=-1, slot=msg.slot, value=decided, view=self.view))
            self._send(Message(MsgType.CLIENT_RESP, src=self.id, dst=msg.src, n=-1, slot=msg.slot, value=decided, view=self.view))
    
    # Invio messaggi protetto dal flag di crash
    def _send(self, msg: Message):
        if not self._crashed:
            self.network.send(msg.src, msg.dst, msg)

async def demo(num_nodes: int = 5, num_decisions: int = 10, drop_prob: float = 0.05, dup_prob: float = 0.02, base_latency: int=0, jitter: int=0):
    node_ids = [f"n{i+1}" for i in range(num_nodes)]
    majority = num_nodes // 2 + 1
    net = Network(base_latency_ms=base_latency, jitter_ms=jitter, drop_prob=drop_prob, dup_prob=dup_prob, seed=123)
    net.start()

    nodes = [MPaxosNode(nid, node_ids, net, majority) for nid in node_ids]
    for n in nodes: n.start()

    await asyncio.sleep(1.2) #aspetta per l'elezione del leader
    net.msg_count = 0
    
    async def client_task(client_node: MPaxosNode, prefix: str): #fa partire una task client con K decisioni
        for s in range(1, num_decisions + 1):
            val = f"{prefix}{s}"
            await client_node.submit(s, val)

    start_time = time.time()
    
    tasks = [asyncio.create_task(client_task(nodes[0], "A"))] #il nodo n1 riceve le richieste del client
    await asyncio.gather(*tasks) #attendiamo che la task termini
    
    await asyncio.sleep(0.1)

    end_time = time.time()
    elapsed = end_time - start_time
    
    total_ops = len(nodes[0].log) 
    tps = total_ops / elapsed if elapsed > 0 else 0 #Transazioni per secondo
    total_msgs = net.msg_count

    for n in nodes: await n.stop()
    await net.stop()

    logger.info(f"METRICS -> Time: {elapsed:.4f}s | Msgs: {total_msgs} | TPS: {tps:.2f} \n")
    return elapsed, total_msgs, tps

# Verifica la natura CFT di Multi-Paxos con crash del leader
async def demo_multipaxos_cft():
    logger.info("=== TEST CFT MULTI-PAXOS: Leader Crash & Recovery ===")
    num_nodes = 5
    node_ids = [f"n{i+1}" for i in range(num_nodes)]
    majority = num_nodes // 2 + 1
    net = Network(base_latency_ms=10, jitter_ms=5, seed=123)
    net.start()

    nodes = [MPaxosNode(nid, node_ids, net, majority) for nid in node_ids]
    for n in nodes: n.start()

    await asyncio.sleep(1.5) # Attesa elezione primo leader (solitamente n5)
    
    current_leader = next(n for n in nodes if n.leader_id == n.id)
    logger.info(f"Leader iniziale eletto: {current_leader.id}")

    # Slot 1: Funzionamento normale
    logger.info("--- Slot 1: Proposta normale ---")
    await nodes[0].submit(1, "Val-1")
    
    # CRASH del Leader attuale
    logger.info(f"--- Simulo CRASH del leader {current_leader.id} ---")
    current_leader.crash()

    # Slot 2: Il sistema deve accorgersi del crash, eleggere un nuovo leader e decidere
    logger.info("--- Slot 2: Proposta durante recovery ---")
    start_recovery = time.time()
    result = await nodes[0].submit(2, "Val-2")
    
    elapsed = time.time() - start_recovery
    new_leader = next(n for n in nodes if n.leader_id == n.id and not n._crashed)
    
    logger.info(f"Deciso Slot 2: {result} | Nuovo Leader: {new_leader.id} | Tempo recovery: {elapsed:.2f}s")

    for n in nodes: await n.stop()
    await net.stop()

if __name__ == "__main__":
    asyncio.run(demo(num_decisions=10))
    asyncio.run(demo_multipaxos_cft())