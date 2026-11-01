/* fixed.c - Producer-consumer buffer data sensor, mutex + semaphore */
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <time.h>

#define BUF_SIZE 16
#define N_PROD   2
#define N_CONS   2
#define ITEMS    100000
#define TOTAL    (N_PROD * ITEMS)

int buffer[BUF_SIZE];
int in = 0, out = 0, count = 0;
pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER; /* kunci buffer,in,out */
sem_t kosong;                    /* jumlah slot kosong (awal BUF_SIZE) */
sem_t terisi;                    /* jumlah slot terisi (awal 0)        */
int seen[TOTAL + 1];
long long sum_prod = 0, sum_cons[N_CONS];
long cons_n[N_CONS];

static double now(void) {
    struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec + t.tv_nsec / 1e9;
}

void *producer(void *arg) {
    int id = (int)(long)arg;
    long long local = 0;
    for (int i = 0; i < ITEMS; i++) {
        int v = id * ITEMS + i + 1;
        sem_wait(&kosong);                  /* tunggu slot kosong */
        pthread_mutex_lock(&mtx);           /* masuk critical section */
        buffer[in] = v;
        in = (in + 1) % BUF_SIZE;
        count++;
        pthread_mutex_unlock(&mtx);         /* keluar critical section */
        sem_post(&terisi);                  /* beri tahu: ada data baru */
        local += v;
    }
    __sync_fetch_and_add(&sum_prod, local);
    return NULL;
}

void *consumer(void *arg) {
    int id = (int)(long)arg;
    long long local = 0; long n = 0;
    for (int i = 0; i < TOTAL / N_CONS; i++) {
        sem_wait(&terisi);                  /* tunggu ada data */
        pthread_mutex_lock(&mtx);
        int v = buffer[out];
        out = (out + 1) % BUF_SIZE;
        count--;
        pthread_mutex_unlock(&mtx);
        sem_post(&kosong);                  /* beri tahu: ada slot kosong */
        __sync_fetch_and_add(&seen[v], 1);
        local += v; n++;
    }
    sum_cons[id] = local; cons_n[id] = n;
    return NULL;
}

int main(void) {
    pthread_t p[N_PROD], c[N_CONS];
    sem_init(&kosong, 0, BUF_SIZE);
    sem_init(&terisi, 0, 0);
    double t0 = now();
    for (long i = 0; i < N_PROD; i++)
        pthread_create(&p[i], NULL, producer, (void *)i);
    for (long i = 0; i < N_CONS; i++)
        pthread_create(&c[i], NULL, consumer, (void *)i);
    for (int i = 0; i < N_PROD; i++) pthread_join(p[i], NULL);
    for (int i = 0; i < N_CONS; i++) pthread_join(c[i], NULL);
    double dt = now() - t0;

    long dikirim = 0, hilang = 0, duplikat = 0; long long sum_c = 0;
    for (int i = 0; i < N_CONS; i++) {
        dikirim += cons_n[i]; sum_c += sum_cons[i];
    }
    for (int v = 1; v <= TOTAL; v++) {
        if (seen[v] == 0) hilang++;
        else if (seen[v] > 1) duplikat += seen[v] - 1;
    }
    printf("=== VERSI DENGAN MUTEX + SEMAPHORE ===\n");
    printf("Data diharapkan   : %d\n", TOTAL);
    printf("Data terkirim     : %ld\n", dikirim);
    printf("Data hilang       : %ld\n", hilang);
    printf("Data duplikat     : %ld\n", duplikat);
    printf("count akhir       : %d (seharusnya 0)\n", count);
    printf("Checksum produsen : %lld\n", sum_prod);
    printf("Checksum konsumen : %lld\n", sum_c);
    printf("Status            : %s\n",
           (hilang == 0 && duplikat == 0 && count == 0 && sum_prod == sum_c)
           ? "KONSISTEN" : "RACE CONDITION TERDETEKSI");
    printf("Waktu             : %.3f detik\n", dt);
    return 0;
}
