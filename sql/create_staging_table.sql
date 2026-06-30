CREATE TABLE IF NOT EXISTS staging.transactions (

    step INTEGER,

    transaction_type VARCHAR(30),

    amount NUMERIC(18,2),

    amount_category VARCHAR(20),

    source_account VARCHAR(50),

    destination_account VARCHAR(50),

    fraud_status VARCHAR(20),

    load_timestamp TIMESTAMP

);