# Docker Network Architecture

## Network Configuration

```yaml
networks:
  main-network:
    # Kafka ↔ Spark communication
    # Schema Registry, Redpanda Console access
    
  kafka-internal-network:
    # Controllers ↔ Brokers only
    # Internal = true (isolated)
    
  spark-network:
    # Spark Master ↔ Workers
    # Dedicated for Spark cluster communication
```

## Connectivity Matrix

| Service | main-network | kafka-internal | spark-network |
|---------|-------------|----------------|---------------|
| **Controllers** | ❌ | ✅ | ❌ |
| **Brokers** | ✅ | ✅ | ❌ |
| **Schema Registry** | ✅ | ❌ | ❌ |
| **Redpanda Console** | ✅ | ❌ | ❌ |
| **Spark Master** | ✅ | ❌ | ✅ |
| **Spark Workers** | ✅ | ❌ | ✅ |

## Port Mappings

| Service | URL | Port |
|---------|-----|------|
| **Spark Master UI** | http://localhost:9090 | 9090 |
| **Spark Worker 1 UI** | http://localhost:8082 | 8082 |
| **Spark Worker 2 UI** | http://localhost:8083 | 8083 |
| **Spark Worker 3 UI** | http://localhost:8084 | 8084 |
| **Schema Registry** | http://localhost:18081 | 18081 |
| **Redpanda Console** | http://localhost:8080 | 8080 |
| **Kafka Broker 1** | localhost:29092 | 29092 |
| **Kafka Broker 2** | localhost:39092 | 39092 |
| **Kafka Broker 3** | localhost:49092 | 49092 |
```