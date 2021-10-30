
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


