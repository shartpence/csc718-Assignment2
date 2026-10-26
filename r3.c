#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <math.h>
#include <mpi.h>

#define TAG_WORK    1
#define TAG_STOP    2
#define TAG_RESULT  3
#define TAG_REQUEST 4  // for R2 dynamic pull model
//compile: mpicc -o r1 r1.c -lm
//run: mpirun -np 4 ./r1 200000 50000  takes 2 argument, total size of the work N followed by chunk size CHUNK

// Optional compile-time flag for logging
#define DEBUG_LOG 0//initialize as 0 for Timing Experiments!!!

// Helper function to test primality
bool is_prime(long long n) {
    if (n < 2) return false;
    if (n == 2 || n == 3) return true;
    if (n % 2 == 0 || n % 3 == 0) return false;
    for (long long i = 5; i * i <= n; i += 6) {
        if (n % i == 0 || n % (i + 2) == 0) return false;
    }
    return true;
}

// Sequential helper for verification
long long count_primes_seq(long long start, long long end) {
    long long count = 0;
    for (long long i = start; i <= end; i++) {
        if (is_prime(i)) count++;
    }
    return count;
}

void run_master(long long N, long long chunk_size, int num_workers) {
    //change in logic from static
    //static uses 3 loops to assign, collect and end work
    //dynamic uses one state-driven event loop
    long long current_start=2;
    int active_workers = num_workers;
    long long total_primes_parallel = 0;

    while (active_workers > 0) {
        long long buffer[2]; // Stores incoming payload
        MPI_Status status;

        // 1. Receive ANY incoming message from ANY worker
        MPI_Recv(buffer, 2, MPI_LONG_LONG, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status); 
        
        int sender = status.MPI_SOURCE;
        int tag = status.MPI_TAG;

        // 2. Handle Work Requests
        if (tag == TAG_REQUEST) {
            if (current_start <= N) {
                // Calculate chunk boundary
                long long end = current_start + chunk_size - 1;
                if (end > N) {
                    end = N;
                }

                long long range[2] = {current_start, end};

                if (DEBUG_LOG) {
                    printf("Master: send [%lld..%lld] to worker %d\n", current_start, end, sender);
                }

                // Send chunk directly to the requesting worker
                MPI_Send(range, 2, MPI_LONG_LONG, sender, TAG_WORK, MPI_COMM_WORLD);

                // Advance global work cursor
                current_start += chunk_size;
            } else {
                // No work remaining: Tell this specific worker to stop
                long long dummy_range[2] = {0, 0};
                MPI_Send(dummy_range, 2, MPI_LONG_LONG, sender, TAG_STOP, MPI_COMM_WORLD);
                
                // Mark this worker as terminated
                active_workers--;
            }
        } 
        
        // 3. Handle Computed Results
        else if (tag == TAG_RESULT) {
            // Buffer[0] contains the partial_count sent by the worker
            total_primes_parallel += buffer[0];
        }
    }

    printf("Parallel Result: Total primes in [2, %lld] = %lld\n", N, total_primes_parallel);

    // 4. Sequential Verification for small N
    if (N <= 200000) {
        long long seq_count = count_primes_seq(2, N);
        printf("Sequential Verification: Total primes = %lld\n", seq_count);
        
        if (seq_count == total_primes_parallel) {
            printf("VERIFICATION PASSED!\n");
        } else {
            printf("VERIFICATION FAILED! (Parallel: %lld vs Seq: %lld)\n", total_primes_parallel, seq_count);
        }
    }
}

void run_worker(int rank) {
    //chnages from passive
    //workers waite to revice work from master...
    //in active, idle workers request work from master
    //start by sending requests from master
    //reciev commands, do work, send send work,repeat
    while (1) {
    
        //request work from master
        long long dummy = 0;
        MPI_Send(&dummy, 1, MPI_LONG_LONG, 0, TAG_REQUEST, MPI_COMM_WORLD);

        //recieve command from master
        long long range[2];
        MPI_Status status;
        MPI_Recv(range, 2, MPI_LONG_LONG, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        if(status.MPI_TAG==TAG_STOP){
            break;
        }
        if(status.MPI_TAG==TAG_WORK){
            long long partial_count = count_primes_seq(range[0],range[1]);
            //send results back to master
            MPI_Send(&partial_count, 1, MPI_LONG_LONG, 0, TAG_RESULT, MPI_COMM_WORLD);

        }

    
        
    }
}
int main(int argc, char** argv) {
    int rank, size;
    char hostname[256];

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // Get the hostname of the current node/VM
    if (gethostname(hostname, sizeof(hostname)) != 0) {
        snprintf(hostname, sizeof(hostname), "unknown_host");
    }

    // Proof of Placement: Every process prints rank, total size, and hostname
    printf("[Placement] Rank %d of %d running on host: %s\n", rank, size, hostname);
    fflush(stdout);

    // Synchronize processes before timing starts
    MPI_Barrier(MPI_COMM_WORLD);

    // Parse command-line arguments (N and CHUNK)
    long long N = (argc > 1) ? atoll(argv[1]) : 100000000;
    long long chunk_size = (argc > 2) ? atoll(argv[2]) : 50000;

    double start_time = 0.0;

    if (rank == 0) {
        // Start timer on master immediately before dynamic work distribution
        start_time = MPI_Wtime();
        run_master(N, chunk_size, size - 1);
        double end_time = MPI_Wtime();
        
        double execution_time = end_time - start_time;
        printf("\n=== R2 Dynamic Benchmark Summary ===\n");
        printf("Processes (Total): %d (1 Master, %d Workers)\n", size, size - 1);
        printf("N = %lld, CHUNK = %lld\n", N, chunk_size);
        printf("Execution Time: %.6f seconds\n", execution_time);
    } else {
        run_worker(rank);
    }

    MPI_Finalize();
    return 0;
}