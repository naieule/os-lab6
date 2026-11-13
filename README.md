# os-lab6

### 1. Which part of the code opens the socket where the server is going to listen for new connections?

> Line 107 babble_server_implement.c

### 2. Which part of the code manages new connections on the server side?

> Line 147 babble_server_implement.c

### 3. What are the major steps that are run on the server when it receives a message from the client?

> It receives the new command (new_command), checks for errors and directs it through a switch to its respective cid function in process_command.

### 4. Why are LOGIN messages managed differently from other commands?

> LOGIN introduces a new client. They are only executed once per client and fill a new client_bundle_t struct connecting it to a socket.

### 5. What is the purpose of the registration_table?

> Tracks current clients through a list of pointers to client_bundle_t.

### 6. How are keys used on the server?

> Keys are a unique hash of the client name used to identify and refer to them.

### 7. How are the answers to client requests implemented on the server?

> Through checking a parameter on command_t that defines if a request needs an answer from the server.

### 8. What happens on the execution of a FOLLOW request?

> Checks if client (A) key exists, computes hash of client (B) to follow, looks it up and confirms if it exists. Checks if B is already followed by A, if not adds 1 to nb_followers of A and adds B client to its followers list (followers[i]). [run_follow_command]

### 9. What happens on the execution of a PUBLISH request?

> Adds msg to the client timeline, checks for disconections and if an acknowledgement is expected prints it. [run_publish_command]

### 10. What happens on the execution of a TIMELINE request?

> Calls function to timeline_generate_summary to print the last 4 messages of clients followed. [run_timeline_command]

### 11. What are the high-level functions provided to communicate over the network? What is their return value?

## 5.1.1 - 2.a

> FOLLOW: read received client and writing on the followed's client_bundle
> PUBLISH: writing on client_bundle of received client (cl) and all its followers, update for disconnected client of cl
> TIMELINE: reading timeline and clearing it on the client_bundle

For the independent requests of multiple client threads to run in parallel, there is the need of a lock per client. To implement this we chose to use mutexes saved in a struct (lock_struct) with both the lock and the client key. This structures are saved in a buffer (buffer_lock).

We didn't choose read-writer locks as they wouldn't do much benefit since all operations are basically writing operations.

There is also the need to use a mutex to synchronize action on the buffer_lock.

There are 3 operations with the need for client locks and, consequently, the buffer_lock. 

(1) When a client is created we take the following steps:
(i) Create structure for the lock while initializing it
(ii) Adding this struct to the 1st available spot in buffer_lock

(2) When locking a client,
(i) We look for the lock of the corresponding client on the buffer, searching by its key
(ii) We take in the lock as a variable and use it to lock and unlock

(3) When a client is removed

