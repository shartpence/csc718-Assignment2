#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <math.h>
#include <mpi.h>

#define TAG_WORK   1
#define TAG_STOP   2
#define TAG_RESULT 3
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
    long long total_segments = 0;
    // 1. Calculate and push all chunks round-robin using MPI_Send
    for(long long start = 2; start <=N; start+=chunk_size){
        long long end = start + chunk_size-1;
        if(end>N){
            end = N;
        }
        
        int worker_rank = (total_segments%num_workers)+1;
        long long range[2] = {start, end};

        if(DEBUG_LOG){
            printf("Master: send [%lld..%lld] to worker %d\n", start, end, worker_rank);
        }

        MPI_Send(range, 2, MPI_LONG_LONG, worker_rank, TAG_WORK, MPI_COMM_WORLD);

        total_segments++;

    }
    // 2. Send TAG_STOP to all workers
    for(int w = 1; w<=num_workers; w++){
        long long dummy_range[2]={0,0};
        MPI_Send(dummy_range, 2, MPI_LONG_LONG, w, TAG_STOP, MPI_COMM_WORLD);
    }
    // 3. Collect prime counts from all chunks using MPI_Recv
    long long total_primes_parallel = 0;
    for (long long i = 0; i < total_segments; i++) {
        long long partial_count = 0;
        MPI_Status status;

        // Receive partial result from ANY worker as they finish
        MPI_Recv(&partial_count, 1, MPI_LONG_LONG, MPI_ANY_SOURCE, TAG_RESULT, MPI_COMM_WORLD, &status);
        
        total_primes_parallel += partial_count;
    }

    printf("Parallel Result: Total primes in [2, %lld] = %lld\n", N, total_primes_parallel);
    // 4. Perform sequential check if N <= 200000 and verify match
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
    while (1) {
        long long range[2];
        MPI_Status status; // Corrected struct type

        // Block until Master sends a work chunk or stop signal
        MPI_Recv(range, 2, MPI_LONG_LONG, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        if (status.MPI_TAG == TAG_STOP) {
            break; // Exit worker loop cleanly
        }

        if (status.MPI_TAG == TAG_WORK) {
            long long start = range[0];
            long long end = range[1];

            long long partial_count = count_primes_seq(start, end);

            if (DEBUG_LOG) {
                printf("worker %d: processed [%lld..%lld] -> %lld primes\n", 
                       rank, start, end, partial_count);
            }

            // Send partial prime count back to master
            MPI_Send(&partial_count, 1, MPI_LONG_LONG, 0, TAG_RESULT, MPI_COMM_WORLD);
        }
    }
}
int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (size < 2) {
        if (rank == 0) {
            fprintf(stderr, "Error: Must run with at least 2 processes (1 master, 1 worker).\n");
        }
        MPI_Finalize();
        return 1;
    }

    if (argc < 3) {
        if (rank == 0) {
            fprintf(stderr, "Usage: %s N CHUNK\n", argv[0]);
        }
        MPI_Finalize();
        return 1;
    }

    long long N = atoll(argv[1]);
    long long chunk_size = atoll(argv[2]);

    if (N < 2) {
        if (rank == 0) {
            printf("N < 2: Total primes = 0\n");
        }
        MPI_Finalize();
        return 0;
    }

    if (rank == 0) {
        double start_time=MPI_Wtime();
        run_master(N, chunk_size, size - 1);
        double end_time=MPI_Wtime();
        printf("R1 Execution Time: %.6f seconds\n", end_time - start_time);
    } else {
        run_worker(rank);
    }

    MPI_Finalize();
    return 0;
}