import matplotlib.pyplot as plt
import asyncio
import numpy as np

# IMPORT DI TUTTI I PROTOCOLLI
import Paxos
import MultiPaxos
import PBFT
import FullBFTPaxos

async def run_scalability_test():
    print("\n" + "="*70)
    print("      SCALABILITY ANALYSIS (Msg Overhead & Throughput)")
    print("="*70)
    
    node_counts = [5, 7]
    K_DECISIONS = 1 # Carico significativo per stabilizzare il throughput
    baseL = 0
    jitt = 0
    
    msg_results = { "Basic Paxos": [], "Multi-Paxos": [], "PBFT (3-Phase)": [], "Full BFT Paxos (2-Phase)": [] }
    tps_results = { "Basic Paxos": [], "Multi-Paxos": [], "PBFT (3-Phase)": [], "Full BFT Paxos (2-Phase)": [] }

    for n in node_counts:
        print(f"\n>>> TESTING WITH N={n} NODES (K={K_DECISIONS})...")
        
        # Basic Paxos
        _, msg, tps = await Paxos.demo(num_nodes=n, num_decisions=K_DECISIONS, drop_prob=0.0, dup_prob=0.0,base_latency=baseL, jitter=jitt)
        msg_results["Basic Paxos"].append(msg / K_DECISIONS)
        tps_results["Basic Paxos"].append(tps)
        
        # Multi-Paxos
        _, msg, tps = await MultiPaxos.demo(num_nodes=n, num_decisions=K_DECISIONS, drop_prob=0.0, dup_prob=0.0,base_latency=baseL, jitter=jitt)
        msg_results["Multi-Paxos"].append(msg / K_DECISIONS)
        tps_results["Multi-Paxos"].append(tps)
        
        # PBFT
        _, msg, tps = await PBFT.demo_benchmark_dynamic(num_nodes=n, num_decisions=K_DECISIONS,base_latency=baseL, jitter=jitt)
        msg_results["PBFT (3-Phase)"].append(msg / K_DECISIONS)
        tps_results["PBFT (3-Phase)"].append(tps)

        # Full BFT Paxos
        _, msg, tps = await FullBFTPaxos.demo_benchmark_full(num_nodes=n, num_decisions=K_DECISIONS,base_latency=baseL, jitter=jitt)
        msg_results["Full BFT Paxos (2-Phase)"].append(msg / K_DECISIONS)
        tps_results["Full BFT Paxos (2-Phase)"].append(tps)

    return node_counts, msg_results, tps_results

async def run_failure_cost_test():
    print("\n" + "="*60)
    print("      COST OF FAILURE ANALYSIS (PBFT vs Full BFT Paxos)")
    print("="*60)
    
    # --- PBFT Analysis ---
    print(">>> [PBFT] Measuring Steady State Latency...")
    # Usiamo K=10 per media veloce su steady state
    elapsed, _, _ = await PBFT.demo_benchmark_dynamic(num_nodes=4, num_decisions=10)
    pbft_steady = elapsed / 10.0 
    
    print(">>> [PBFT] Measuring Recovery Latency...")
    pbft_crash, _ = await PBFT.demo_liveness() 
    
    # --- Full BFT Paxos Analysis ---
    print(">>> [Full BFT Paxos] Measuring Steady State Latency...")
    elapsed, _, _ = await FullBFTPaxos.demo_benchmark_full(num_nodes=4, num_decisions=10)
    auth_steady = elapsed / 10.0
    
    print(">>> [Full BFT Paxos] Measuring Recovery Latency...")
    auth_crash, _ = await FullBFTPaxos.demo_liveness_full()

    return pbft_steady, pbft_crash, auth_steady, auth_crash

def plot_analytics(n_counts, msg_data, tps_data, pbft_steady, pbft_crash, auth_steady, auth_crash):
    styles = {
        "Basic Paxos":          {'color': 'salmon',       'marker': 's', 'style': '--'},
        "Multi-Paxos":          {'color': 'limegreen',    'marker': 'o', 'style': '-'},
        "PBFT (3-Phase)":       {'color': 'mediumpurple', 'marker': '^', 'style': '-'},
        "Full BFT Paxos (2-Phase)": {'color': 'gold',     'marker': 'D', 'style': '-.'}
    }

    # --- GRAFICO 1: SCALABILITÀ MESSAGGI ---
    plt.figure(figsize=(10, 6))
    for algo, vals in msg_data.items():
        s = styles[algo]
        plt.plot(n_counts, vals, label=algo, color=s['color'], marker=s['marker'], linestyle=s['style'], linewidth=2)
    plt.title("Scalabilità Overhead: Messaggi per Decisione", fontsize=14)
    plt.xlabel("Numero di Nodi (N)", fontsize=12)
    plt.ylabel("Msg/Tx (Normalizzati)", fontsize=12)
    plt.xticks(n_counts)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    plt.show()

    # --- GRAFICO 2: SCALABILITÀ THROUGHPUT ---
    plt.figure(figsize=(10, 6))
    for algo, vals in tps_data.items():
        s = styles[algo]
        plt.plot(n_counts, vals, label=algo, color=s['color'], marker=s['marker'], linestyle=s['style'], linewidth=2)
    plt.title("Scalabilità Performance: Throughput Reale", fontsize=14)
    plt.xlabel("Numero di Nodi (N)", fontsize=12)
    plt.ylabel("Decisioni al Secondo (Dec/s)", fontsize=12)
    plt.xticks(n_counts)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    plt.show()

    # --- GRAFICO 3: COSTO DEL GUASTO (Comparison) ---
    plt.figure(figsize=(10, 6))
    
    # Dati
    labels = ['Steady State (Avg)', 'Leader Crash Recovery']
    pbft_vals = [pbft_steady, pbft_crash]
    auth_vals = [auth_steady, auth_crash]
    
    x = np.arange(len(labels))  
    width = 0.35  # Larghezza barre

    # Creazione barre raggruppate
    ax = plt.gca()
    rects1 = ax.bar(x - width/2, pbft_vals, width, label='PBFT (3-Phase)', color='mediumpurple', edgecolor='black', alpha=0.8)
    rects2 = ax.bar(x + width/2, auth_vals, width, label='Full BFT Paxos (2-Phase)', color='gold', edgecolor='black', alpha=0.8)

    # Scala logaritmica
    ax.set_yscale('log')
    ax.set_ylabel('Latenza (Secondi) - Scala Logaritmica', fontsize=12)
    ax.set_title('Il Costo del Guasto: PBFT vs Full BFT Paxos', fontsize=14)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=11)
    ax.legend()
    ax.grid(axis='y', linestyle='--', alpha=0.5)

    # Funzione helper per etichette
    def autolabel(rects):
        for rect in rects:
            height = rect.get_height()
            ax.annotate(f'{height:.2f}s',
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),  # 3 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom', fontweight='bold', fontsize=10)

    autolabel(rects1)
    autolabel(rects2)

    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    try:
        nodes_x, msg_res, tps_res = asyncio.run(run_scalability_test())
        pbft_s, pbft_c, auth_s, auth_c = asyncio.run(run_failure_cost_test())
        
        print("\n>>> Generazione Grafici...")
        plot_analytics(nodes_x, msg_res, tps_res, pbft_s, pbft_c, auth_s, auth_c)
        
    except KeyboardInterrupt:
        pass