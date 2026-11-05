# os-lab6

1. line 107 babble_server_implement.c

2. line 147 babble_server_implement.c

3. It receives the new command (new_command), checks for errors and directs it through a switch to its respective cid function in process_command.

4. LOGIN introduces a new client. They are only executed once per client and fill a new client_bundle_t struct connecting it to a socket.

5. Tracks current clients through a list of pointers to client_bundle_t.

6. Keys are a unique hash of the client name used to identify and refer to them.

7. Through checking a parameter on command_t that defines if a request needs an answer from the server.

8. Checks if client (A) key exists, computes hash of client (B) to follow, looks it up and confirms if it exists. Checks if B is already followed by A, if not adds 1 to nb_followers of A and adds B client to its followers list (followers[i]). [run_follow_command]

9. Adds msg to the client timeline, checks for disconections and if an acknowledgement is expected prints it. [run_publish_command]

10. Calls function to timeline_generate_summary to print the last 4 messages of clients followed. [run_timeline_command]

11. 
