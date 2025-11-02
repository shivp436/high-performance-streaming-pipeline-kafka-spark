# High Performance DE Pipeline - with Apache Kafka, Spark


## <u>Docker Network Architecture:</u>

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

## <u>Storage Overhead Calculation:</u>

## Record-Level Analysis

### Single Record Structure
| Field | Type | Example | Size (bytes) |
|-------|------|---------|-------------|
| `transaction_id` | string (UUID) | `550e8400-e29b-41d4-a716-446655440000` | 36 |
| `amount` | float | `1234.5678` | 8 |
| `user_id` | string | `user1234` | 12 |
| `transaction_time` | bigint (ms) | `1704067200000` | 8 |
| `merchant_id` | string | `merchant1` | 12 |
| `transaction_type` | string | `purchase` | 8 |
| `location` | string | `location1` | 12 |
| `payment_method` | string | `bank_transfer` | 15 |
| `is_international` | bool | `true` | 4 |
| `currency` | string | `gbp` | 5 |

**Total per record:** 120 bytes

## Hourly Calculation

```
Records per hour: 1.2 billion
Raw data size: 1,200,000,000 × 120 bytes = 144,000,000,000 bytes = 144 GB
```

## Storage Overhead Factors

### With Infrastructure Considerations
```
Raw hourly data: 144 GB
+ Kafka overhead (headers, etc.): ~50% → 216 GB
× Replication factor: 3
÷ Compression ratio: 5x (Snappy/gzip)
```

**Effective hourly storage:** `216 × 3 ÷ 5 = 130 GB/hour`

## Time-Based Scaling

### Daily Storage
```
130 GB/hour × 24 hours = 3,120 GB/day = 3.12 TB/day
```

### Annual Storage
```
3.12 TB/day × 365 days = 1,138.8 TB/year ≈ 1,112 TB/year
```

## Growth Projection

Using compound growth formula:
```
Size_nthYear = Base Data Size × (1 + Growth Factor)^n
```

### Year-over-Year Projection (20% Growth)

| Year | Calculation | Storage Required |
|------|-------------|------------------|
| **Year 1** | `1,112 × (1.2)^1` | **1,334 TB** |
| **Year 2** | `1,112 × (1.2)^2` | **1,601 TB** |
| **Year 3** | `1,112 × (1.2)^3` | **1,921 TB** |
| **Year 4** | `1,112 × (1.2)^4` | **2,305 TB** |
| **Year 5** | `1,112 × (1.2)^5` | **2,766 TB** |

## Summary

| Metric | Value |
|--------|-------|
| **Records per second** | ~333,333 |
| **Raw data ingress** | 144 GB/hour |
| **Effective storage** | 130 GB/hour |
| **Daily storage** | 3.12 TB/day |
| **Year 1 capacity** | 1,334 TB |
| **Year 3 capacity** | 1,921 TB |
| **5-year growth** | 2.5× increase |
```

**Note:** This calculation assumes:
- Consistent record volume (1.2B/hour sustained)
- 20% annual growth rate
- 5x compression efficiency maintained
- 3x replication for data durability
- 50% Kafka overhead for headers and metadata