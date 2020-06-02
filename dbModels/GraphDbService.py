from neo4j import (
    GraphDatabase,
    TRUST_SYSTEM_CA_SIGNED_CERTIFICATES
)

from neobolt.exceptions import ServiceUnavailable

driver_config = {
    "encrypted": False,
    "trust": TRUST_SYSTEM_CA_SIGNED_CERTIFICATES,
    "user_agent": "example",
    "max_connection_lifetime": 1000,
    "max_connection_pool_size": 100,
    "keep_alive": False,
    "max_transaction_retry_time": 10,
    "resolver": None,
}


class GraphDbService:

    def __init__(self, uri, user, password, verbose=0):
        try:
            self.driver = GraphDatabase.driver(uri, auth=(user, password), **driver_config)
            self.verbose = verbose

        except ServiceUnavailable as e:
            print(str(e))

    def close(self):
        self.driver.close()
