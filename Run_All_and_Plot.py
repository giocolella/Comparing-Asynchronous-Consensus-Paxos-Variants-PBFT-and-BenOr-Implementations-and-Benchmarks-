import matplotlib.pyplot as plt
import asyncio
import time
import Paxos
import MultiPaxos
import PBFT       
import FullBFTPaxos 
import BenOr

async def run_full_suite():
    print("\n" + "#"*70)
    print("       STARTING DISTRIBUTED CONSENSUS TEST SUITE (5 PROTOCOLS)")
    print("#"*70 + "\n")

    # Standardizzazione Benchmark
    N_UNIFORM = 5 
    K_DECISIONS = 1000
    baseL = 0
    jitt = 0

    print(f">>> [1/5] RUNNING BASIC PAXOS (N={N_UNIFORM}, K={K_DECISIONS})...")
    paxos_lat, paxos_msgs, paxos_tps = await Paxos.demo(
        num_nodes=N_UNIFORM, num_decisions=K_DECISIONS, drop_prob=0.0, dup_prob=0.0,base_latency=baseL, jitter=jitt
    )
    print(f"    Done. Latency: {paxos_lat:.2f}s\n")
    await asyncio.sleep(1)

    print(f">>> [2/5] RUNNING MULTI-PAXOS (N={N_UNIFORM}, K={K_DECISIONS})...")
    mp_lat, mp_msgs, mp_tps = await MultiPaxos.demo(
        num_nodes=N_UNIFORM, num_decisions=K_DECISIONS, drop_prob=0.0, dup_prob=0.0,base_latency=baseL, jitter=jitt
    )
    print(f"    Done. Latency: {mp_lat:.2f}s\n")
    await asyncio.sleep(1)

    print(f">>> [3/5] RUNNING PBFT (3-Phase, N={N_UNIFORM}, K={K_DECISIONS})...")
    pbft_lat, pbft_msgs, pbft_tps = await PBFT.demo_benchmark_dynamic(
        num_nodes=N_UNIFORM, num_decisions=K_DECISIONS,base_latency=baseL, jitter=jitt
    )
    print(f"    Done. Latency: {pbft_lat:.2f}s\n")
    await asyncio.sleep(1)

    print(f">>> [4/5] RUNNING FULL BFT PAXOS (2-Phase, N={N_UNIFORM}, K={K_DECISIONS})...")
    auth_lat, auth_msgs, auth_tps = await FullBFTPaxos.demo_benchmark_full(
        num_nodes=N_UNIFORM, num_decisions=K_DECISIONS,base_latency=baseL, jitter=jitt
    )
    print(f"    Done. Latency: {auth_lat:.2f}s\n")
    await asyncio.sleep(1)

    print(f">>> [5/5] RUNNING BEN-OR (Probabilistic, N={N_UNIFORM}, K={K_DECISIONS})...")
    # Ben-Or richiede drop_prob=0.0 per liveness teorica
    benor_lat, benor_msgs, benor_tps = await BenOr.demo_benor(
        num_nodes=N_UNIFORM, consensus_iterations=K_DECISIONS, drop_prob=0.0, base_latency=baseL, jitter=jitt
    )
    print(f"    Done. Latency: {benor_lat:.2f}s\n")
    await asyncio.sleep(1)

    print("\n" + "="*40)
    print("      RUNNING ROBUSTNESS CHECKS")
    print("="*40)
    
    print(">>> [PBFT] Checking Safety (Byzantine Liar)...")
    pbft_safe, pbft_safe_note = await PBFT.demo_safety()
    print(">>> [PBFT] Checking Liveness (Leader Crash)...")
    pbft_rec_time, pbft_rec_note = await PBFT.demo_liveness()
    
    print(">>> [Full BFT Paxos] Checking Safety (Equivocation)...")
    auth_safe, auth_safe_note = await FullBFTPaxos.demo_safety()
    print(">>> [Full BFT Paxos] Checking Liveness (Leader Crash)...")
    auth_rec_time, auth_rec_note = await FullBFTPaxos.demo_liveness_full()

    perf_data = {
        "Basic Paxos":   {"lat": paxos_lat, "msgs": paxos_msgs, "tps": paxos_tps},
        "Multi-Paxos":   {"lat": mp_lat, "msgs": mp_msgs, "tps": mp_tps},
        "PBFT (3-Phase)":{"lat": pbft_lat, "msgs": pbft_msgs, "tps": pbft_tps},
        "Full BFT Paxos (2-Phase)": {"lat": auth_lat, "msgs": auth_msgs, "tps": auth_tps},
        "Ben-Or (Probabilistic)":   {"lat": benor_lat, "msgs": benor_msgs, "tps": benor_tps}
    }
    
    pbft_rob = {
        "Safety": "PASSED" if pbft_safe else "FAILED",
        "Liveness": f"RECOVERED ({pbft_rec_time:.2f}s)" if pbft_rec_time > 0 else "FAILED"
    }
    
    auth_rob = {
        "Safety": "PASSED" if auth_safe else "FAILED",
        "Liveness": f"{auth_rec_note} ({auth_rec_time:.2f}s)"
    }
    
    return perf_data, pbft_rob, auth_rob, K_DECISIONS

def plot_suite_results(perf_data, pbft_rob, auth_rob, k_dec):
    algos = list(perf_data.keys())
    lats = [perf_data[a]["lat"] for a in algos]
    msgs = [perf_data[a]["msgs"] for a in algos]
    
    colors = ['salmon', 'limegreen', 'mediumpurple', 'gold', 'skyblue']

    plt.figure(figsize=(14, 6))
    bars = plt.bar(algos, lats, color=colors, edgecolor='black', alpha=0.8)
    plt.title(f"Tempo Totale per completare {k_dec} Decisioni (N=5)", fontsize=14)
    plt.ylabel("Secondi", fontsize=12)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 0.02, 
                 f"{height:.2f}s", ha='center', va='bottom', fontweight='bold')
    plt.show()

    plt.figure(figsize=(14, 6))
    plt.plot(algos, msgs, marker='o', linestyle='-', linewidth=2, color='gray', zorder=1)
    plt.bar(algos, msgs, color=colors, alpha=0.3, zorder=0)
    plt.title(f"Messaggi Totali scambiati per {k_dec} Decisioni (N=5)", fontsize=14)
    plt.ylabel("Numero Messaggi", fontsize=12)
    plt.grid(True)
    for i, v in enumerate(msgs):
        plt.text(i, v + 10, str(v), ha='center', fontweight='bold')
    plt.show()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 3))
    
    ax1.axis('tight')
    ax1.axis('off')
    ax1.set_title("PBFT (Deterministic)", fontsize=12, fontweight='bold')
    tbl1_data = [
        ["Test Case", "Result"],
        ["Safety (Liar)", pbft_rob["Safety"]],
        ["Liveness (Crash)", pbft_rob["Liveness"]]
    ]
    t1 = ax1.table(cellText=tbl1_data, loc='center', cellLoc='center', colWidths=[0.5, 0.5])
    t1.auto_set_font_size(False)
    t1.set_fontsize(11)
    t1.scale(1, 1.5)
    for row in range(1, 3):
        res = tbl1_data[row][1]
        color = "#ccffcc" if "PASSED" in res or "RECOVERED" in res else "#ffcccc"
        t1.get_celld()[(row, 1)].set_facecolor(color)

    ax2.axis('tight')
    ax2.axis('off')
    ax2.set_title("Full BFT Paxos (Optimistic)", fontsize=12, fontweight='bold')
    tbl2_data = [
        ["Test Case", "Result"],
        ["Safety (Equivocation)", auth_rob["Safety"]],
        ["Liveness (Crash)", auth_rob["Liveness"]]
    ]
    t2 = ax2.table(cellText=tbl2_data, loc='center', cellLoc='center', colWidths=[0.5, 0.5])
    t2.auto_set_font_size(False)
    t2.set_fontsize(11)
    t2.scale(1, 1.5)
    for row in range(1, 3):
        res = tbl2_data[row][1]
        color = "#ccffcc" if "PASSED" in res or "RECOVERED" in res else "#ffcccc"
        t2.get_celld()[(row, 1)].set_facecolor(color)

    plt.show()

if __name__ == "__main__":
    try:
        p_data, p_rob, a_rob, k_val = asyncio.run(run_full_suite())
        plot_suite_results(p_data, p_rob, a_rob, k_val)
    except KeyboardInterrupt:
        pass