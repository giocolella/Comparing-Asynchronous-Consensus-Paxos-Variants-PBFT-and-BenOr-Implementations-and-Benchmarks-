# ⚡ Comparing Asynchronous Consensus: Paxos Variants, PBFT and Ben-Or
**Implementations and Benchmarks in Simulated Asynchronous Networks**

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg)]()
[![Asyncio](https://img.shields.io/badge/Asyncio-Concurrency-green.svg)]()
[![Consensus](https://img.shields.io/badge/Distributed-Systems-orange.svg)]()
[![License](https://img.shields.io/badge/License-MIT-lightgrey.svg)]()

---

## 📑 Table of Contents
1. [Overview](#-overview)
2. [Motivation](#-motivation--why-this-project)
3. [Implemented Protocols](#-implemented-protocols)
4. [Simulation & Benchmarking](#-simulation--benchmarking)
5. [Requirements](#-requirements)
6. [Setup & Usage](#-setup--usage)
7. [Tools & References](#-tools--references)

---

## 📌 Overview
This project provides a comprehensive implementation and comparative analysis of five major distributed consensus algorithms. It utilizes a custom-built **asynchronous network simulator** in Python to evaluate performance (throughput/latency) and robustness (CFT vs BFT) under various network conditions.

The goal is to show how these protocols behave in real-world scenarios characterized by latency, jitter, packet loss, and node failures.

---

## 🎯 Motivation / Why This Project?
- 🧠 **Deep Dive into Consensus:** Moving beyond theory to understand the implementation details of Paxos, PBFT, and Randomized Consensus.
- 🛡 **CFT vs. BFT:** Comparing Crash Fault Tolerance (Paxos) against Byzantine Fault Tolerance (PBFT, Ben-Or) to measure the "cost" of security.
- 📉 **Performance Analysis:** Visualizing how latency and message overhead scale with the number of nodes.
- 🎲 **Deterministic vs. Probabilistic:** Contrasting rigid multi-phase commits against the randomized approach of Ben-Or.

---

## ⚙ Implemented Protocols

#### **1. Basic Paxos** (`Paxos.py`)
- The classic single-decree consensus algorithm by Leslie Lamport.
- **Type:** CFT (Crash Fault Tolerant).
- **Phases:** 2 (Prepare/Promise -> Accept/Accepted).
- **Characteristics:** Leaderless per-instance, high latency in high contention.

#### **2. Multi-Paxos** (`MultiPaxos.py`)
- An optimized version of Paxos for streams of values.
- **Type:** CFT.
- **Features:** Stable Leader Election (Bully Algorithm), Heartbeats, Fast Path (skipping Phase 1).
- **Performance:** Highest throughput among CFT protocols.

#### **3. PBFT (Practical Byzantine Fault Tolerance)** (`PBFT.py`)
- The standard for tolerating malicious nodes.
- **Type:** BFT (Byzantine Fault Tolerant).
- **Phases:** 3 (PrePrepare -> Prepare -> Commit).
- **Security:** Uses RSA signatures to prevent spoofing and tampering.

#### **4. Full BFT Paxos** (`FullBFTPaxos.py`)
- An optimized BFT variant inspired by authenticated Paxos.
- **Type:** BFT.
- **Phases:** 2 (Propose -> Accept).
- **Features:** Relies on non-repudiation (Signatures) to reduce message rounds compared to PBFT.

#### **5. Ben-Or Randomized Consensus** (`BenOr.py`)
- A probabilistic protocol for asynchronous systems (FLP impossibility result workaround).
- **Type:** BFT.
- **Mechanism:** Uses local coin flips to break symmetry when consensus stalls.
- **Characteristics:** Theoretical termination with probability 1, non-deterministic latency.

---

## 📊 Simulation & Benchmarking

The project includes a robust simulation engine (`Network` class) that replicates real-world network imperfections:
- **Latency & Jitter:** Configurable base latency and random variation.
- **Packet Loss:** Probabilistic message dropping (`drop_prob`).
- **Failure Simulation:** Ability to crash nodes or inject Byzantine behaviors (Lying, Equivocation, Silence).

### **Benchmark Suites**
- **`Run_All_and_Plot.py`**: Runs a full suite of performance tests (N=5) and Robustness checks (Safety/Liveness), generating comparative charts.
- **`Run_Analytics.py`**: Performs scalability testing (N=5 vs N=7) and analyzes the "Cost of Failure" (Recovery time vs Steady state).

---

## 💻 Requirements

### **Software**
- Python 3.8 or higher
- Pip (Python Package Installer)

### **Python Libraries**
The project relies on standard libraries (`asyncio`, `random`, `logging`) and a few external packages for plotting and cryptography:

```text
matplotlib
numpy
cryptography
```

## 📦 Setup & Usage

### **1. Clone the Repository**
Open your terminal and clone the repository to your local machine:

```bash
git clone [https://github.com/giocolella/Comparing-Asynchronous-Consensus-Paxos-Variants-PBFT-and-BenOr-Implementations-and-Benchmarks-.git](https://github.com/giocolella/Comparing-Asynchronous-Consensus-Paxos-Variants-PBFT-and-BenOr-Implementations-and-Benchmarks-.git)
cd Comparing-Asynchronous-Consensus-Paxos-Variants-PBFT-and-BenOr-Implementations-and-Benchmarks-
```

### **2. Install Dependencies**
Open your terminal and clone the repository to your local machine:

```bash
pip install matplotlib numpy cryptography
```

### **3. Run the Main Benchmark**
To execute the comparison of all 5 protocols (Basic Paxos, Multi-Paxos, PBFT, Full BFT Paxos, Ben-Or) and generate the performance/robustness plots:

```bash
python Run_All_and_Plot.py
```

### **4. Run Scalability Analytics**
To analyze how the protocols scale when increasing nodes (e.g., N=5 vs N=7) and to visualize the "Cost of Failure" (Steady State vs Recovery Time):

```bash
python Run_Analytics.py
```

## 📚 Tools & Documentation
[Paxos Made Simple (Leslie Lamport)](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf)

[Practical Byzantine Fault Tolerance (Castro & Liskov)](https://dl.acm.org/doi/10.1145/571637.571640)

[Another Advantage of Free Choice (Ben-Or)](https://dl.acm.org/doi/10.1145/800221.806707)

[Python Asyncio Documentation](https://docs.python.org/3/library/asyncio.html)

[Python Cryptography Library](https://cryptography.io/en/latest/)
