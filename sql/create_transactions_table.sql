CREATE TABLE IF NOT EXISTS raw.transactions (
    step INTEGER,
    transaction_type VARCHAR(50),
    amount NUMERIC(15,2),

    source_account VARCHAR(50),
    source_old_balance NUMERIC(15,2),
    source_new_balance NUMERIC(15,2),

    destination_account VARCHAR(50),
    destination_old_balance NUMERIC(15,2),
    destination_new_balance NUMERIC(15,2),

    is_fraud INTEGER,
    is_flagged_fraud INTEGER
);