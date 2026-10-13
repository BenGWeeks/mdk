-- Public, validated advisory display metadata; independent of replaceable KP slots.
CREATE TABLE device_client_labels (
    account_id_hex TEXT NOT NULL,
    signature_key BLOB NOT NULL,
    client_label TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    confirmed_order INTEGER NOT NULL,
    PRIMARY KEY (account_id_hex, signature_key)
);
