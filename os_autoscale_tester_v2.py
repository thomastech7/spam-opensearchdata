import argparse
import time
import random
from datetime import datetime, timezone
import logging
import urllib3

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
    """Initializes the OpenSearch client with retry logic and dynamic HTTP/HTTPS support."""
    while True:
        try:
            # Base connection arguments
            client_kwargs = {
                'hosts': [{'host': args.host, 'port': args.port}],
                'http_auth': (args.user, args.password),
                'timeout': 10
            }
            
            # Apply SSL-specific arguments only if HTTPS is chosen
            if args.scheme == 'https':
                client_kwargs.update({
                    'use_ssl': True,
                    'verify_certs': False,  # Ignore self-signed certs for local IPs
                    'ssl_assert_hostname': False,
                    'ssl_show_warn': False
                })
            else:
                client_kwargs.update({
                    'use_ssl': False
                })

            client = OpenSearch(**client_kwargs)
            
            # Ping to verify connection
            if client.ping():
                return client
            else:
                raise ConnectionError("Ping failed")
        except Exception as err:
            msg = f"Connection failed: {err}. Retrying in 5 seconds..."
            print(f"\n[!] {msg}")
            logging.error(msg)
            time.sleep(5)

def get_index_size_mb(client, index_name):
    """Queries the OpenSearch _stats API for the accurate index size."""
    try:
        stats = client.indices.stats(index=index_name)
        bytes_size = stats['indices'][index_name]['total']['store']['size_in_bytes']
        return round(bytes_size / (1024 * 1024), 2)
    except Exception as err:
        return -1

def main():
    parser = argparse.ArgumentParser(description="OpenSearch Load Generator with Autoscaling Resilience")
    parser.add_argument('--scheme', choices=['http', 'https'], default='https', help="Connection scheme (http or https)")
    parser.add_argument('--host', required=True, help="OpenSearch IP or Hostname")
    parser.add_argument('--port', type=int, default=9200, help="OpenSearch Port")
    parser.add_argument('--index', required=True, help="Index Name")
    parser.add_argument('--user', required=True, help="OpenSearch Username")
    parser.add_argument('--password', required=True, help="OpenSearch Password")
    parser.add_argument('--batch-size', type=int, default=500, help="Number of logs to insert per bulk request")
    parser.add_argument('--delay', type=float, default=0.5, help="Delay (in seconds) between bulk requests")
    parser.add_argument('--size-check-interval', type=int, default=20, help="Check index size every N batches")
    parser.add_argument('--log-file', default='os_autoscale_events.txt', help="File to save script logs")

    args = parser.parse_args()

    # Set up the logger
    logging.basicConfig(
        filename=args.log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    print(f"Connecting to {args.scheme}://{args.host}:{args.port}...")
    client = get_client(args)

    # Create index if it doesn't exist (Ignore 400 'resource_already_exists_exception')
    try:
        client.indices.create(index=args.index, ignore=400)
        print(f"Index '{args.index}' is ready.")
    except Exception as err:
        print(f"Failed to set up index: {err}")
        return

    start_msg = f"Started OpenSearch load test on {args.scheme}://{args.host}. Batch size: {args.batch_size}, Delay: {args.delay}s"
    print(f"{start_msg}\nLogs are being written to: {args.log_file}")
    print("Press Ctrl+C to stop the load test.\n")
    logging.info(start_msg)

    total_inserted = 0
    batch_count = 0
    current_size_mb = get_index_size_mb(client, args.index)

    while True:
        try:
            now_str = datetime.now().strftime('%H:%M:%S')
            
            # Prepare the Bulk API payload
            actions = [
                {
                    "_index": args.index,
                    "_source": generate_pseudo_log()
                }
                for _ in range(args.batch_size)
            ]

            # Execute the bulk request
            success, failed = helpers.bulk(client, actions, stats_only=True)
            
            total_inserted += success
            batch_count += 1

            # Check size periodically
            if batch_count % args.size_check_interval == 0:
                current_size_mb = get_index_size_mb(client, args.index)

            print(f"[{now_str}] Inserted: {total_inserted} docs | Accurate Size: {current_size_mb} MB", end='\r')
            time.sleep(args.delay)

        except (OpenSearchException, ConnectionError, TransportError) as err:
            error_msg = f"Connection dropped (Autoscaling/Failover likely). Error: {err}"
            print(f"\n\n[!] {error_msg}")
            print("[*] Yielding for 10 seconds to allow OpenSearch cluster to stabilize...")
            logging.warning(error_msg)
            
            time.sleep(10)
            
            print("[*] Attempting to reconnect...")
            client = get_client(args)
            
            reconnect_msg = "Reconnected successfully. Resuming load test."
            print(f"[*] {reconnect_msg}\n")
            logging.info(reconnect_msg)

        except KeyboardInterrupt:
            stop_msg = f"Load test stopped by user. Total documents indexed: {total_inserted}"
            print(f"\n\n{stop_msg}")
            logging.info(stop_msg)
            break

if __name__ == "__main__":
    main()