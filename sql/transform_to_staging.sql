TRUNCATE TABLE staging.transactions;

INSERT INTO staging.transactions (

    step,
    transaction_type,
    amount,
    amount_category,
    source_account,
    destination_account,
    fraud_status,
    load_timestamp

)

SELECT

    step,

    transaction_type,

    amount,

    CASE
        WHEN amount < 1000 THEN 'SMALL'
        WHEN amount < 10000 THEN 'MEDIUM'
        ELSE 'LARGE'
    END,

    source_account,

    destination_account,

    CASE
        WHEN is_fraud = 1 THEN 'Fraud'
        ELSE 'Genuine'
    END,

    CURRENT_TIMESTAMP

FROM raw.transactions;