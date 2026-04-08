import argparse
import time
import random
from datetime import datetime, timezone
import logging
import urllib3
import multiprocessing
import sys

from opensearchpy import OpenSearch, helpers
from opensearchpy.exceptions import OpenSearchException, ConnectionError, TransportError

# Suppress insecure SSL warnings for local IP testing
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def generate_pseudo_log():
    """Generates a JSON document mimicking a web server access log."""
    methods = ['GET', 'POST', 'PUT', 'DELETE']
    paths = ['/api/v1/data', '/login', '/checkout', '/images/logo.png', '/search?q=test']
    statuses = [200, 200, 200, 201, 400, 403, 404, 500]
    agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        "curl/7.68.0"
    ]
    
    return {
        "@timestamp": datetime.now(timezone.utc).isoformat(),
        "client_ip": f"10.0.{random.randint(1, 255)}.{random.randint(1, 255)}",
        "method": random.choice(methods),
        "request_path": random.choice(paths),
        "status_code": random.choice(statuses),
        "response_time_ms": random.randint(10, 500),
        "user_agent": random.choice(agents),
        "bytes_sent": random.randint(120, 5000)
    }

def get_client(args):
    """Initializes the OpenSearch client with dynamic HTTP/HTTPS support."""
    client_kwargs = {
        'hosts': [{'host': args.host, 'port': args.port}],
        'http_auth': (args.user, args.password),
        'timeout': 30  
    }
    
    if args.scheme == 'https':
        client_kwargs.update({
            'use_ssl': True,
            'verify_certs': False,
            'ssl_assert_hostname': False,
            'ssl_show_warn': False
        })
    else:
        client_kwargs.update({'use_ssl': False})

    return OpenSearch(**client_kwargs)

def load_generator_worker(worker_id, args, stop_event, shared_counter):
    """The function executed by each multiprocessing worker."""
    client = get_client(args)
    
    while not stop_event.is_set():
        try:
            actions = [
                {"_index": args.index, "_source": generate_pseudo_log()}
                for _ in range(args.batch_size)
            ]
            # Execute the bulk request
            helpers.bulk(client, actions, stats_only=True)
            
            # Safely increment the shared counter across processes
            with shared_counter.get_lock():
                shared_counter.value += args.batch_size
                
            time.sleep(args.delay)
            
        except Exception as err:
            # If the cluster drops connection, back off temporarily
            time.sleep(5)
            client = get_client(args)

def main():
    parser = argparse.ArgumentParser(description="Multicore OpenSearch Load Generator (Local Tracking)")
    parser.add_argument('--scheme', choices=['http', 'https'], default='https', help="Connection scheme")
    parser.add_argument('--host', required=True, help="OpenSearch IP or Hostname")
    parser.add_argument('--port', type=int, default=9200, help="OpenSearch Port")
    parser.add_argument('--index', required=True, help="Index Name")
    parser.add_argument('--user', required=True, help="OpenSearch Username")
    parser.add_argument('--password', required=True, help="OpenSearch Password")
    parser.add_argument('--batch-size', type=int, default=1000, help="Logs per bulk request per worker")
    parser.add_argument('--delay', type=float, default=0.1, help="Delay between bulk requests per worker")
    parser.add_argument('--workers', type=int, default=4, help="Number of concurrent processes to spawn")
    parser.add_argument('--log-file', default='os_autoscale_events.txt', help="File to save script logs")
    parser.add_argument('--skip-create-index', action='store_true', help="Skip explicit index creation")

    args = parser.parse_args()

    logging.basicConfig(
        filename=args.log_file, level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'
    )

    print(f"Connecting to {args.scheme}://{args.host}:{args.port} to verify cluster...")
    main_client = get_client(args)
    
    try:
        if not main_client.ping():
            raise Exception("Ping returned False. Server rejected the connection.")
            
        if not args.skip_create_index:
            try:
                main_client.indices.create(index=args.index, ignore=[400, 403])
            except Exception:
                pass
        else:
            print("[*] Skipping index creation step as requested.")
            
    except Exception as err:
        print(f"[!] Initialization failed: {err}")
        sys.exit(1)

    print(f"\n--- Load Test Initialized ---")
    print(f"Target: {args.scheme}://{args.host}:{args.port}/{args.index}")
    print(f"Workers: {args.workers} concurrent processes")
    print(f"Payload: {args.batch_size} docs/batch per worker")
    print("Press Ctrl+C to stop.\n")
    logging.info(f"Started multicore load test with {args.workers} workers.")

    # Multiprocessing setup
    stop_event = multiprocessing.Event()
    # Create a shared variable (unsigned long long) to count documents safely across all workers
    total_docs_inserted = multiprocessing.Value('Q', 0) 
    processes = []

    # Spawn the workers, passing the shared counter to them
    for i in range(args.workers):
        p = multiprocessing.Process(target=load_generator_worker, args=(i, args, stop_event, total_docs_inserted))
        p.start()
        processes.append(p)

    # Main process monitoring loop
    try:
        while True:
            time.sleep(1) # Update the console every 1 second
            now_str = datetime.now().strftime('%H:%M:%S')
            
            # Read the current value of the shared counter
            current_count = total_docs_inserted.value
            
            print(f"[{now_str}] Live Progress -> Total Docs Sent by Script: {current_count:,}", end='\r')

    except KeyboardInterrupt:
        print("\n\n[*] Stopping all workers gracefully (this may take a few seconds)...")
        final_count = total_docs_inserted.value
        logging.info(f"Load test stopped by user. Total docs sent: {final_count}")
        stop_event.set()
        
        for p in processes:
            p.join()
        print(f"All processes terminated. Final Document Count: {final_count:,}")

if __name__ == "__main__":
    main()