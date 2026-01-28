"""
Ben-Or Randomized Consensus
---------------------------
Implementazione basata sulla descrizione teorica (Sezione 6.5.2).
Algoritmo per sistemi asincroni con terminazione probabilistica.

Requisiti: n > 2f
Valori: Binari {0, 1}

Integration: Uses the shared 'Network' class logic from BFTPaxos/MultiPaxos.
"""
import asyncio
import dataclasses
import enum
import logging
import random
import time
import heapq
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Set

# Configurazione Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("BenOr")

# --- Network Infrastructure (Identica a BFTPaxos per equità di confronto) ---
class Network:
    def __init__(self, drop_prob=0.0, seed=42):
        self.inboxes = {}
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
        # Latenza tipica simulata (15ms - 20ms)
        delay = 0.015 + self._rand.uniform(0, 0.005)
        self._seq += 1
        heapq.heappush(self._pq, (time.time() + delay, self._seq, dst, payload))

# --- Definitions ---

class MsgType(enum.Enum):
    FIRST = "FIRST"    # Round 1
    SECOND = "SECOND"  # Round 2
    DECIDE = "DECIDE"  # Messaggio extra per propagare la decisione

@dataclasses.dataclass(frozen=True)
class BenOrMessage:
    mtype: MsgType
    src: str
    phase: int
    value: Optional[int]  # 0, 1, o None (per null)

class BenOrNode:
    def __init__(self, node_id: str, all_nodes: List[str], network: Network, initial_value: int):
        self.id = node_id
        self.all_nodes = all_nodes
        self.N = len(all_nodes)
        # Dall'immagine: "Tale algoritmo richiede che n > 2f" => f < n/2
        self.f = (self.N - 1) // 2
        self.network = network
        self.inbox = network.register(node_id)
        
        self.x = initial_value  # Valore di preferenza corrente (variabile x nell'immagine)
        self.phase = 1          # Fase corrente
        self.decided = False
        self.decision = None
        
        # Buffer per messaggi di fasi future (essenziale in sistemi asincroni)
        # phase -> type -> list[msg]
        self.future_msg_buffer = defaultdict(lambda: defaultdict(list))
        
        self._stop = False
        self._task = None

    def start(self):
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._stop = True
        if self._task: await self._task

    def broadcast(self, mtype: MsgType, val: Optional[int]):
        """Invia un messaggio a tutti i nodi (incluso se stesso)."""
        msg = BenOrMessage(mtype, self.id, self.phase, val)
        for dst in self.all_nodes:
            self.network.send(self.id, dst, msg)

    async def _collect_messages(self, target_phase: int, mtype: MsgType, required_count: int) -> List[BenOrMessage]:
        """
        Raccoglie 'required_count' messaggi del tipo e fase specificati.
        Gestisce il buffering di messaggi futuri.
        """
        collected = []
        
        # 1. Controlla prima nel buffer se abbiamo già messaggi per questa fase
        if target_phase in self.future_msg_buffer and mtype in self.future_msg_buffer[target_phase]:
            # Sposta i messaggi dal buffer alla lista raccolta
            available = self.future_msg_buffer[target_phase][mtype]
            while available and len(collected) < required_count:
                collected.append(available.pop(0))
        
        # 2. Se non bastano, aspetta dalla rete
        while len(collected) < required_count and not self._stop:
            try:
                msg: BenOrMessage = await asyncio.wait_for(self.inbox.get(), timeout=0.1)
                
                if msg.mtype == MsgType.DECIDE:
                    # Se qualcuno ha deciso, decidiamo anche noi immediatamente (Ottimizzazione comune)
                    if not self.decided:
                        logger.info(f"{self.id} received DECIDE({msg.value}) from {msg.src}. Terminating.")
                        self.decided = True
                        self.decision = msg.value
                    continue

                if msg.phase < target_phase:
                    continue # Scarta messaggi vecchi
                
                if msg.phase > target_phase:
                    # Messaggio futuro: bufferizza
                    self.future_msg_buffer[msg.phase][msg.mtype].append(msg)
                    continue
                
                if msg.mtype != mtype:
                    # Stessa fase ma tipo diverso (es. ricevuto SECOND mentre aspetto FIRST): bufferizza
                    self.future_msg_buffer[msg.phase][msg.mtype].append(msg)
                    continue

                # Messaggio corretto
                collected.append(msg)

            except asyncio.TimeoutError:
                continue
        
        return collected

    async def _run(self):
        """Loop principale dell'algoritmo (per fasi)."""
        # Ritardo casuale all'avvio per desincronizzare leggermente e rendere lo scenario più realistico
        await asyncio.sleep(random.uniform(0, 0.05))

        while not self.decided and not self._stop:
            # logger.debug(f"{self.id} starting Phase {self.phase} with x={self.x}")
            
            # --- ROUND 1 ---
            # "Spedisce in broadcast il messaggio (first, s, x)"
            self.broadcast(MsgType.FIRST, self.x)

            # "Aspetta l'arrivo di almeno n - f messaggi della forma (first, s, v)"
            msgs1 = await self._collect_messages(self.phase, MsgType.FIRST, self.N - self.f)
            if self.decided: break

            # "Se almeno floor(n/2) + 1 hanno tutti lo stesso valore v allora y := v, altrimenti y := null"
            y = None
            counts1 = defaultdict(int)
            for m in msgs1:
                counts1[m.value] += 1
            
            threshold_r1 = (self.N // 2) + 1
            for val, count in counts1.items():
                if count >= threshold_r1:
                    y = val
                    break
            
            # --- ROUND 2 ---
            # "Spedisce in broadcast il messaggio (second, s, y)"
            self.broadcast(MsgType.SECOND, y)

            # "Aspetta l'arrivo di almeno n - f messaggi della forma (second, s, v)"
            msgs2 = await self._collect_messages(self.phase, MsgType.SECOND, self.N - self.f)
            if self.decided: break

            # Logica di transizione e decisione
            
            # Conta occorrenze dei valori nel round 2
            # Nota: i valori possono essere 0, 1 o None (null)
            vals_non_null = [m.value for m in msgs2 if m.value is not None]
            
            # (a) "Se c'è un messaggio con un valore v != null allora x := v"
            # (Se ce n'è più di uno diverso, l'algoritmo standard prende uno qualsiasi valido. 
            #  Data la logica del round 1, se esistono valori non-null, devono essere uguali tra nodi onesti).
            if vals_non_null:
                self.x = vals_non_null[0] # Aggiorna la preferenza per la prossima fase
            
            # (b) "Se almeno f + 1 messaggi contengono uno stesso valore v allora il processore decide v"
            counts2 = defaultdict(int)
            for m in msgs2:
                if m.value is not None:
                    counts2[m.value] += 1
            
            decided_in_round = False
            for val, count in counts2.items():
                if count >= self.f + 1:
                    self.decided = True
                    self.decision = val
                    decided_in_round = True
                    logger.info(f"{self.id} DECIDED {val} at Phase {self.phase}")
                    # Broadcast finale per informare gli altri (Best Effort per terminazione globale)
                    msg_decide = BenOrMessage(MsgType.DECIDE, self.id, 0, val)
                    for dst in self.all_nodes: self.network.send(self.id, dst, msg_decide)
                    break
            
            # (c) "Altrimenti, si sceglie uniformemente a caso 0 o 1 come valore di x"
            if not decided_in_round and not vals_non_null:
                # Se non ho visto nessun valore valido (tutti null), lancio la moneta
                self.x = random.choice([0, 1])
                # logger.debug(f"{self.id} flipped coin -> {self.x}")

            self.phase += 1

# --- Demo & Benchmark Wrapper ---

async def demo_benor(num_nodes=5, consensus_iterations=1):
    """
    Esegue un benchmark di Ben-Or.
    Poiché Ben-Or è binario, per renderlo 'comparabile' agli altri che fanno K decisioni,
    eseguiamo K istanze di consenso binario sequenziali.
    """
    logger.info(f"=== BEN-OR CONSENSUS (N={num_nodes}, Iterations={consensus_iterations}) ===")
    
    # 1. Setup Rete
    net = Network(drop_prob=0.0, seed=random.randint(0, 1000))
    node_ids = [f"n{i}" for i in range(num_nodes)]
    
    # 2. Esecuzione Sequenziale (per simulare stream di decisioni)
    total_start = time.time()
    
    for k in range(consensus_iterations):
        # Input casuali per i nodi (0 o 1)
        inputs = [random.choice([0, 1]) for _ in range(num_nodes)]
        # Forza un input misto per vedere la convergenza, oppure uniforme per il caso ottimo
        # inputs = [1, 0, 1, 0, 1][:num_nodes] 
        
        nodes = []
        for i, nid in enumerate(node_ids):
            n = BenOrNode(nid, node_ids, net, initial_value=inputs[i])
            nodes.append(n)
        
        for n in nodes: n.start()
        
        # Attesa terminazione
        while True:
            done_count = sum(1 for n in nodes if n.decided)
            if done_count == num_nodes:
                # Check consistency
                decisions = set(n.decision for n in nodes)
                if len(decisions) > 1:
                    logger.error(f"FATAL: Safety violation! Decisions: {decisions}")
                break
            await asyncio.sleep(0.01)
        
        # Pulizia per prossima iterazione
        for n in nodes: await n.stop()
        # logger.info(f"Iteration {k+1}/{consensus_iterations} completed.")

    total_end = time.time()
    await net.stop()
    
    elapsed = total_end - total_start
    tps = consensus_iterations / elapsed if elapsed > 0 else 0
    logger.info(f"Total Time: {elapsed:.4f}s | Msg Count: {net.msg_count} | TPS: {tps:.2f}")
    
    return elapsed, net.msg_count, tps

if __name__ == "__main__":
    # Test rapido standalone
    asyncio.run(demo_benor(num_nodes=4, consensus_iterations=5))