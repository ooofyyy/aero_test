create table sandbox.cbt (
    id UInt32,
    uid String,
    strain String,
    cannabinoid_abbreviation String,
    cannabinoid String,
    terpene String,
    medical_use String,
    health_benefit String,
    category String,
    type String,
    buzzword String,
    brand String,
    stamp DateTime DEFAULT now()
    )
ENGINE = ReplacingMergeTree
ORDER BY (id,uid)