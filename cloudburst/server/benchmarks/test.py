warmup_keys = 1000
tx_size = 2
dag_size = 6
for i in range(warmup_keys // tx_size):
    keys_to_read = []
    ref_to_key = []
    for j in range(tx_size):
        keys_to_read.append("k" + str(i*tx_size + j))
    for k in range(1, dag_size):
        next_request = []
        for m in range(tx_size):
            next_request.append("k" + str( (i * tx_size + k * tx_size + m) % warmup_keys ))
        ref_to_key.append(next_request)
    print(keys_to_read)
    print(ref_to_key)
    print()
