#include <stdio.h>
#include <string.h>
#include <pthread.h>

#include "babble_registration.h"

client_bundle_t *registration_table[MAX_CLIENT];
int nb_registered_clients;
pthread_rwlock_t lock_rw = PTHREAD_RWLOCK_INITIALIZER;


void registration_init(void)
{
    nb_registered_clients=0;

    memset(registration_table, 0, MAX_CLIENT * sizeof(client_bundle_t*));
}

client_bundle_t* registration_lookup(unsigned long key)
{
    int i=0;
    client_bundle_t *c = NULL;
    pthread_rwlock_rdlock(&lock_rw);

    for(i=0; i< nb_registered_clients; i++){
        if(registration_table[i]->key == key){
            c = registration_table[i];
            break;
        }
    }
    pthread_rwlock_unlock(&lock_rw);
    return c;
}

int registration_insert(client_bundle_t* cl)
{    
    pthread_rwlock_wrlock(&lock_rw);
    if(nb_registered_clients == MAX_CLIENT){
        fprintf(stderr, "ERROR: MAX NUMBER OF CLIENTS REACHED\n");
        pthread_rwlock_unlock(&lock_rw);
        return -1;
    }
    
    /* lookup to find if key already exists */
    int i=0;

    
    for(i=0; i< nb_registered_clients; i++){
        if(registration_table[i]->key == cl->key){
            break;
        }
    }
    
    
    if(i != nb_registered_clients){
        fprintf(stderr, "Error -- id % ld already in use\n", cl->key);
        pthread_rwlock_unlock(&lock_rw);
        return -1;
    }

    /* insert cl */
    registration_table[nb_registered_clients]=cl;
    nb_registered_clients++;
    pthread_rwlock_unlock(&lock_rw);
    
    return 0;
}


client_bundle_t* registration_remove(unsigned long key)
{
    pthread_rwlock_wrlock(&lock_rw);
    int i=0;
    
    for(i=0; i<nb_registered_clients; i++){
        if(registration_table[i]->key == key){
            break;
        }
    }

    if(i == nb_registered_clients){
        fprintf(stderr, "Error -- no client found\n");
        pthread_rwlock_unlock(&lock_rw);    
        return NULL;
    }
    
    client_bundle_t* cl= registration_table[i];

    nb_registered_clients--;
    registration_table[i] = registration_table[nb_registered_clients];
    pthread_rwlock_unlock(&lock_rw);
    return cl;
}
