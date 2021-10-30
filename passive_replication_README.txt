
TODO for primary:

make the global variable to local

have three variables x, y, z and each client updates each variable...dont mix this up

have a checkpt counter in primary server

connect primary with the backup servers

send checkpoints....send x, y , z

have a variable am_i_quiet //toggle this variable to do state transition.
                            // when true - respond to clients
                            // when false - send checkpoints

have a checkpoint_msg_count variable // after sending "checkpoint_msg_count" responses change to checkpointing mode (toggle am_i_quiet variable)



TODO for backup:

connect backup with the primary servers

receive checkpoints....x, y, z



IMPLEMENTATION

We will be having two worker threads W1 and W2.

W1 has the connections corresponding to client side (which consists of LFDs and clients together)
W2 has the connections corresponding to backup side

W1 works(responds to client's requests) when am_i_quiet is false....
NOTE: but it will be listening to client's requests even when am_i_quiet is false

W2 works when am_i_quiet is true



New FUNCTIONS added:
basic_primary_server
primary_server_handler
basic_backup_server
backup_server_handler


