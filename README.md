# CSC 718 - Assignment 2: Parallel Prime-Counting MPI Algorithms

This project implements and benchmarks static (R1) and dynamic (R2/R3) master-worker parallel algorithms for counting prime numbers in the range $[2, N]$ using OpenMPI.

---

## 1. Build Instructions

Compile the binaries using `mpicc` with optimization flags and the standard math library (`-lm`):

```bash
# Compile Static Algorithm (R1)
mpicc -O2 -o r1 r1.c -lm

# Compile Dynamic Algorithm (R2)
mpicc -O2 -o r2 r2.c -lm

# Compile Rushmore Scaling Benchmark (R3)
mpicc -O2 -o r3 r3.c -lm

## 2. Run Instructions

Local execution (WSL2/Linux)
Example below to run locally with 4 processes (1 master, 3 workers)

mpirun -np 4 ./r1 100000000 50000
mpirun -np 4 ./r2 100000000 50000

Rushmore Cluster Execution (R3 Scaling Study)
Execution on the Rushmore virtual cluster uses machinefile.dsu to distribute processes across nodes (Lincoln, Washington, Roosevelt, Jefferson):

# 4 Processes (1 process per VM)
mpirun -hostfile machinefile.dsu -np 4 ./r3 100000000 50000

# 8 Processes (2 processes per VM)
mpirun -hostfile machinefile.dsu -np 8 ./r3 100000000 50000

# 16 Processes (4 processes per VM)
mpirun -hostfile machinefile.dsu -np 16 ./r3 100000000 50000

## 3. Command Line Arguments (N and CHUNK)
Executables accept two positional arguments:
./r3 <N> <CHUNK>

    N (Upper Bound): Specifies the range $[2, N]$ of integers to evaluate for primality. Larger values increase overall computational density and workload balance skew.

    CHUNK (Work Unit Size): The number of contiguous integers assigned to a worker in a single work request packet. A smaller CHUNK improves load balancing in dynamic scheduling but increases message-passing overhead; a larger CHUNK reduces communication frequency.

## 4. Description of the Primality Test

Each candidate integer {x|2<=x<=N} is evaluated using trial division:
    1. Numbers less than 2 are non-prime.
    2. 2 is prime; all other even numbers are composite and skipped.
    3. For odd numbers x>=3, trial division tests odd divisors {d|3<=d<=sqrt(x)}. If x%d == 0, x is composite. If no divisor divides x, x is prime.

## 5. Description of Timing Method
    - Execution times are measured using MPI_Wtime() on Rank 0 (Master).

    - Timer Start: Immediately before master dispatches the first work assignment (R1) or enters the message-listening loop (R2/R3).

    - Timer Stop: Immediately after all partial results are collected and stop signals (TAG_STOP) are issued to all workers.

    - Excluded Operations: Program setup, MPI_Init(), host discovery (gethostname()), proof-of-placement console logging, sequential verification checks, and MPI_Finalize() are excluded from timing measurements.

## 6. Assumptions, Limitations, and Implementation Notes
    - Master-Worker Role Separation: Rank 0 acts exclusively as a master
    - dispatcher/aggregator and does not compute prime numbers. Ranks 1...  P-1 act as worker compute processes.
    - Dynamic Work Dispatch (R2/R3): Idle workers issue explicit work request messages to Rank 0, which responds with the next range CHUNK until range N is exhausted.
     - NFS Filesystem Dependency: On Rushmore, execution requires /home/mpiuser/nfs to be mounted across all computing nodes. If node mounting drops, run sudo mount -a as user dsu on the affected VM.
    - Detailed Logging: In-process debug logging is disabled during timing benchmark runs to eliminate console I/O noise.
EOF