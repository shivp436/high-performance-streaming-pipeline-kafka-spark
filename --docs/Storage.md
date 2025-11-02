### How to Calculate storage overhead?

#### Start from lowest granularity

Size of 1 Record:
{
    transaction_id: string -> uuid => 36 bytes
    amount: float -> 1234.5678 => 8 bytes
    user_id: string -> user1234 => 12 bytes
    transaction_time (in ms): bigint => 8 bytes
    merchant_id: string -> merchant1 => 12 bytes
    transaction_type: string -> purchase => 8 bytes
    location: string -> location1 => 12 bytes
    payment_method: string -> bank_transfer => 15 bytes
    is_international: bool -> true => 4 bytes
    currency: string -> gbp => 5 bytes
}

Total: 120 bytes
Hourly record count: 1.2 billion
-----------------------
Hourly data size ~= 216 billion bytes ~= 216GB
-----------------------

With Compression: 5x Compression (Approx. Snappy/gzip)
Replicas: 3 
= 216 * 3 / 5 = 130 GB/Hour 

= 130*24 = 3120 GB/Day 
= 3120*365 = 1138800 GB/Year = 1112 TB/Year

--------------

Size_nthYear = Base Data Size * (1+Growth Factor)^n
With 20% Growth
Year1 = 1112 TB 
Year2 = 1112 * 1.44 = 1601 TB 
Year3 = 1112 * (1.2)^3 = 1921 TB
