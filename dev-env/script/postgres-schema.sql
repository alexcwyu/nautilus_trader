

CREATE TABLE IF NOT EXISTS account
(
    uuid                uuid         NOT NULL,
    user_uuid           uuid         NOT NULL,
    name        varchar(255) NOT NULL,
    CONSTRAINT account_pkey PRIMARY KEY (uuid),
);