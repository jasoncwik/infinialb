from itertools import cycle
import socket
from urllib.parse import urlparse

import urllib3
from botocore.httpsession import URLLib3Session
from urllib3 import PoolManager
from urllib3.util.connection import create_connection


# Resolve IP address(es) for the host name.
def get_ip_addresses(url):
    parsed_url = urlparse(url)
    hostname = parsed_url.hostname
    try:
        # Get all IP addresses associated with the hostname
        ip_addresses = socket.getaddrinfo(hostname, None)
        # Extract unique IP addresses
        unique_ips = set(ip[4][0] for ip in ip_addresses)
        print(f"IP addresses for {hostname}: {unique_ips}")
        return list(unique_ips)
    except socket.gaierror:
        print(f"Could not resolve hostname: {hostname}")
        return []


class CustomPoolManager(PoolManager):
    def __init__(self, ip_addresses, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ip_cycle = cycle(ip_addresses)
        self.pool_classes_by_scheme = {
            'http': CustomHTTPConnectionPool,
            'https': CustomHTTPSConnectionPool,
        }

    def _new_pool(self, scheme, host, port, request_context=None):
        pool_cls = self.pool_classes_by_scheme[scheme]
        return pool_cls(host, port, ip_cycle=self.ip_cycle, **self.connection_pool_kw)


class CustomHTTPConnectionPool(urllib3.HTTPConnectionPool):
    def __init__(self, host, port, ip_cycle, **kwargs):
        super().__init__(host, port, **kwargs)
        self.ip_cycle = ip_cycle

    def _new_conn(self):
        ip = next(self.ip_cycle)
        conn = create_connection((ip, self.port), timeout=self.timeout)
        return conn


class CustomHTTPSConnectionPool(urllib3.HTTPSConnectionPool):
    def __init__(self, host, port, ip_cycle, **kwargs):
        super().__init__(host, port, **kwargs)
        self.ip_cycle = ip_cycle

    def _new_conn(self):
        ip = next(self.ip_cycle)
        conn = create_connection((ip, self.port), timeout=self.timeout)
        return conn


# Takes a Boto S3 client and adds the client-side load balancer
def apply_round_robin_to_client(client):
    endpoint_url = client.meta.endpoint_url
    ip_addresses = get_ip_addresses(endpoint_url)
    if not ip_addresses:
        print(f"No IP addresses found for {endpoint_url}. Round-robin not applied.")
        return client

    print("Found IP addresses: %s" % ip_addresses)
    custom_pool_manager = CustomPoolManager(
        ip_addresses,
        timeout=urllib3.Timeout.DEFAULT_TIMEOUT,
        maxsize=10,
        retries=urllib3.Retry(total=3, backoff_factor=0.2)
    )

    custom_session = URLLib3Session(
        verify=True,  # You might want to make this configurable
        timeout=None,  # Using the default timeout
        max_pool_connections=10,  # Adjust as needed
        socket_options=None,
        client_cert=None,
        proxies_config=None
    )

    # Replace the default pool manager with our custom one
    custom_session._pool_manager = custom_pool_manager

    # Override the client's _get_session method to use our custom session
    client._get_session = lambda: custom_session

    print(f"Round-robin load balancing applied to {client.__class__.__name__} for {endpoint_url}")
    return client
