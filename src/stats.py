import argparse
import time
import resource

from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

def calculate_total_vout(transaction):
    total = 0

    for vout in transaction['vout']:
        total += vout['value']

    return total

def get_rpc_connection(
        user, password, host="127.0.0.1", port="8332", protocol="http"):
    connection_url = "%s://%s:%s@%s:%d"%(protocol, user, password, host, port)
    return AuthServiceProxy(connection_url)

def get_block_info_from(rpc_connection, time):
    '''
    Get the block info since time
    '''
    blocks = list()
    height = rpc_connection.getblockcount()

    while True:
        hash = rpc_connection.getblockhash(height)
        block = rpc_connection.getblock(hash)

        if int(block['time']) < time:
            return blocks
        else:
            blocks.append(block)
            height -= 1

def get_transactions_from(rpc_connection, blocks):
    transactions = list()

    for block in blocks:
        for tx in block['tx']:
            rawtransaction = rpc_connection.getrawtransaction(tx)
            transaction = rpc_connection.decoderawtransaction(rawtransaction)
            transactions.append(transaction)

    return transactions

def find_largest_transaction(transactions):
    largest_tx = None
    largest_value = 0

    for tx in transactions:
        total = calculate_total_vout(tx)

        if total > largest_value:
            largest_tx = tx
            largest_value = total

    return largest_tx

def main(args):
    '''
    Get statistics about the blockchain
    '''
    start_time = time.time()

    try:
        rpc_connection = get_rpc_connection(
                args.user, args.password, args.host, args.port)

        print("Connected to daemon")
    except:
        print("Something went wrong while trying to establish connection:")
        raise

    print("Collecting blocks since %d"%(args.since))
    blocks = get_block_info_from(rpc_connection, args.since)
    print("Found %d blocks"%(len(blocks)))

    print("Collecting transaction data from blocks")
    transactions = get_transactions_from(rpc_connection, blocks)
    print("Found %d transactions"%(len(transactions)))


    average_block_time = (time.time() - args.since) / len(blocks)
    average_tx_per_block = float(len(transactions)) / float(len(blocks))

    transactions_by_value = sorted(transactions, key=calculate_total_vout
            , reverse=True)

    print("") # Print empty line
    print(" --- BLOCKS ---")
    print("Total blocks found:             %d"%(len(blocks)))
    print("Average block time:             %d seconds"%(average_block_time))
    print("Average transactions per block: %f"%(average_tx_per_block))

    print("") # Print empty line
    print(" --- TRANSACTIONS ---")
    print("Total transactions: %d"%(len(transactions)))
    print("Top 10 transactions:")

    for transaction in transactions_by_value[:10]:
        print("    %f %s"
                %(calculate_total_vout(transaction), transaction['txid']))

    print("") # Print empty line
    print(" --- STATISTICS ---")
    print("Runtime in seconds: %d"%(time.time() - start_time))
    print("Memory usage:       %d"%(
        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description='Generate statistics about the blockchain')
    parser.add_argument('--host', type=str, default='127.0.0.1',
            help='Hostname to use (default: 127.0.0.1)')
    parser.add_argument('--port', type=int, default=8332,
            help='Port number to use (default: 8332)')
    parser.add_argument('--user', type=str, help='RPC username to use')
    parser.add_argument('--password', type=str, help='RPC password to use')
    parser.add_argument('--since', type=int,
            default=time.time() - 60 * 60 * 24 * 7,
            help='Time to collect stats from (defaults to past week)')

    main(parser.parse_args())
