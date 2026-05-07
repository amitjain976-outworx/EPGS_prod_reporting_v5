-- =============================================================================
--  EPGS REPORTING GOLD  —  Complete CREATE TABLE Schema  (Latest)
--  Run against: epgs_reporting_gold_v5 (TARGET / local DB)
--  Safe to run on fresh DB — uses CREATE TABLE IF NOT EXISTS
--
--  CHANGES vs previous version (v4):
--    ✅ ADDED   : fact_parking_session.stop_parking_time        DATETIME NULL
--                 Mapped from tickets.stop_parking_time; NULL for overstay/extends.
--
--    ✅ CHANGED : fact_permit_subscription — structural redesign
--       DROPPED : facility_group_key, billed_amount, paid_amount,
--                 balance, spaces_entitled
--       ADDED   : status_date         TIMESTAMP NULL  <- cancelled_at
--       ADDED   : partner_account_key BIGINT NULL     <- via dim_partner_account
--       ADDED   : permit_plan_key     BIGINT NULL     <- via dim_permit_plan
--                                                        (permit_rate_id -> permit_rates
--                                                         -> permit_rate_description_id)
--
--    ✅ CHANGED : fact_payment — new columns
--       CHANGED : processing_fees  now maps to tickets.processing_fee ONLY
--                 (was combined with additional_fee previously)
--       ADDED   : additional_fees         DECIMAL(8,2)  NULL
--                 tickets.additional_fee (SOURCE 1 only; NULL for all other sources)
--       ADDED   : release_surcharge_fee   DECIMAL(10,2) NULL <- tickets.release_surcharge_fee
--       ADDED   : release_tax_fee         DECIMAL(10,2) NULL <- tickets.release_tax_fee
--       ADDED   : release_additional_fee  DECIMAL(10,2) NULL <- tickets.release_additional_fee
--       ADDED   : refund_status           VARCHAR(255)  NULL <- tickets.refund_status
--       ADDED   : refund_release_status   TINYINT(1)    NULL <- tickets.refund_release_status
--       ADDED   : refund_transaction_id   VARCHAR(255)  NULL <- tickets.refund_transaction_id
--       All 7 new fact_payment columns are NULL for non-ticket sources.
--       CHANGED : payment_ts_utc is NULL when source_transaction_id
--                 (anet_transaction_id) is NULL.
-- =============================================================================
CREATE DATABASE IF NOT EXISTS epgs_reporting_gold_v4
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;
USE epgs_reporting_gold_v4;

-- =============================================================================
--  ETL CONTROL TABLES
-- =============================================================================
CREATE TABLE IF NOT EXISTS etl_control (
    load_name          VARCHAR(120)  PRIMARY KEY,
    last_pk            BIGINT        NOT NULL DEFAULT 0,
    last_status        VARCHAR(20)   NOT NULL DEFAULT 'PENDING',
    last_started_at    DATETIME      NULL,
    last_finished_at   DATETIME      NULL,
    last_rows_read     BIGINT        NOT NULL DEFAULT 0,
    last_rows_inserted BIGINT        NOT NULL DEFAULT 0,
    last_message       VARCHAR(500)  NULL,
    updated_at         TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
                                     ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id        BIGINT        AUTO_INCREMENT PRIMARY KEY,
    load_name     VARCHAR(120)  NOT NULL,
    started_at    DATETIME      NOT NULL,
    finished_at   DATETIME      NULL,
    status        VARCHAR(20)   NOT NULL,
    rows_read     BIGINT        NOT NULL DEFAULT 0,
    rows_inserted BIGINT        NOT NULL DEFAULT 0,
    last_pk       BIGINT        NOT NULL DEFAULT 0,
    message       VARCHAR(1000) NULL,
    created_at    TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_rlog_name    (load_name),
    INDEX idx_rlog_status  (status),
    INDEX idx_rlog_started (started_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =============================================================================
--  DIMENSION TABLES
-- =============================================================================
CREATE TABLE IF NOT EXISTS dim_date (
    date_key     INT          PRIMARY KEY,
    full_date    DATE         NOT NULL,
    day_of_month TINYINT,
    day_name     VARCHAR(10),
    day_of_week  TINYINT,
    week_of_year TINYINT,
    month_number TINYINT,
    month_name   VARCHAR(15),
    quarter      TINYINT,
    year         SMALLINT,
    is_weekend   TINYINT(1),
    is_holiday   TINYINT(1)   DEFAULT 0,
    created_at   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
                              ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_time (
    time_key    INT          PRIMARY KEY,
    full_time   TIME         NOT NULL,
    hour        TINYINT,
    minute      TINYINT,
    second      TINYINT,
    am_pm       VARCHAR(2),
    time_bucket VARCHAR(20),
    created_at  TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
                             ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_partner_account (
    partner_account_key  BIGINT       AUTO_INCREMENT PRIMARY KEY,
    account_id_source    INT          NOT NULL,
    account_name         VARCHAR(255),
    account_type         VARCHAR(50),
    partner_id           INT,
    country              VARCHAR(100),
    status               VARCHAR(50),
    created_at           DATETIME,
    updated_at           DATETIME,
    effective_start_date DATETIME     NOT NULL,
    effective_end_date   DATETIME     NOT NULL,
    is_current           TINYINT      DEFAULT 1,
    etl_created_at       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    etl_updated_at       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
                                      ON UPDATE CURRENT_TIMESTAMP,
    record_hash          CHAR(32),
    INDEX idx_partner_business (account_id_source),
    INDEX idx_partner_current  (account_id_source, is_current)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_parker (
    parker_key           BIGINT       AUTO_INCREMENT PRIMARY KEY,
    customer_id          BIGINT,
    customer_type        INT,
    signup_channel       VARCHAR(255),
    parker_name          VARCHAR(255),
    loyalty_tier         TINYINT,
    account_status       VARCHAR(50),
    home_city            VARCHAR(255),
    phone_number         VARCHAR(32),
    email                VARCHAR(255),
    effective_start_date TIMESTAMP,
    created_at           TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
                                      ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_customer_id (customer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_facility (
    facility_key            BIGINT        AUTO_INCREMENT PRIMARY KEY,
    facility_id             INT           NOT NULL,
    facility_name           VARCHAR(255),
    facility_type           VARCHAR(50),
    city                    VARCHAR(255),
    state                   VARCHAR(255),
    country                 VARCHAR(255),
    capacity                INT,
    operator_id             INT,
    garage_code             VARCHAR(50),
    logo                    VARCHAR(255),
    open_time               TIME,
    close_time              TIME,
    latitude                DECIMAL(11,7),
    longitude               DECIMAL(11,7),
    location                VARCHAR(255),
    effective_start_date    DATETIME      NOT NULL,
    effective_end_date      DATETIME      NOT NULL,
    dw_effective_start_date DATETIME      NOT NULL,
    dw_effective_end_date   DATETIME      NOT NULL,
    is_current              BOOLEAN       NOT NULL DEFAULT TRUE,
    created_at              TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
                                          ON UPDATE CURRENT_TIMESTAMP,
    record_hash             CHAR(32),
    INDEX idx_facility_business_key   (facility_id),
    INDEX idx_facility_current_lookup (facility_id, is_current)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_vehicle (
    vehicle_key  BIGINT       AUTO_INCREMENT PRIMARY KEY,
    vehicle_id   INT,
    vehicle_type VARCHAR(255),
    vehicle_code VARCHAR(255),
    is_ev_flag   TINYINT(1),
    created_at   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
                              ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_vehicle (vehicle_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_rateplan (
    rateplan_key         BIGINT        AUTO_INCREMENT PRIMARY KEY,
    pricing_id           INT           NOT NULL,
    rate_plan_name       VARCHAR(255),
    rate_type            VARCHAR(255),
    free_minutes         DECIMAL(8,2),
    max_daily_cap        DECIMAL(8,2),
    base_rate            DECIMAL(8,2),
    is_dynamic_flag      TINYINT,
    effective_start_date DATETIME      NOT NULL,
    effective_end_date   DATETIME      NOT NULL,
    is_current           TINYINT(1)    DEFAULT 1,
    created_at           TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
                                       ON UPDATE CURRENT_TIMESTAMP,
    record_hash          CHAR(32),
    UNIQUE KEY uk_pricing_id    (pricing_id),
    INDEX idx_rateplan_business (pricing_id),
    INDEX idx_rateplan_current  (pricing_id, is_current)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_promo_code (
    promo_key              BIGINT        AUTO_INCREMENT PRIMARY KEY,
    promo_code_id          INT,
    promo_code             VARCHAR(255),
    promo_type             VARCHAR(50),
    discount_type          VARCHAR(20),
    discount_value         DECIMAL(10,2),
    start_date             DATE,
    end_date               DATE,
    is_active              TINYINT,
    is_tax_fees_applicable TINYINT(1),
    facility_id            INT,
    effective_from         DATETIME,
    effective_to           DATETIME,
    effective_start_date   DATETIME      NOT NULL,
    effective_end_date     DATETIME      NOT NULL,
    is_current             TINYINT       DEFAULT 1,
    created_at             TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at             TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
                                         ON UPDATE CURRENT_TIMESTAMP,
    record_hash            CHAR(32),
    INDEX idx_promo_code    (promo_code),
    INDEX idx_promo_current (promo_code, is_current),
    INDEX idx_facility_id   (facility_id),
    UNIQUE KEY uk_promo_current (promo_code, is_current)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_processor (
    processor_key  BIGINT       AUTO_INCREMENT PRIMARY KEY,
    processor_id   INT,
    processor_name VARCHAR(100),
    provider       VARCHAR(100),
    created_at     TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
                                ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_processor (processor_id, processor_name, provider)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_payment_method (
    payment_method_key BIGINT       AUTO_INCREMENT PRIMARY KEY,
    payment_method_id  INT          NOT NULL,
    method_type        VARCHAR(50)  NOT NULL,
    provider_name      VARCHAR(255),
    provider_country   VARCHAR(255) DEFAULT 'United States',
    created_at         TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
                                    ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_payment_method (payment_method_id, method_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_parking_product (
    product_key BIGINT       AUTO_INCREMENT PRIMARY KEY,
    product_id  BIGINT,
    name        VARCHAR(255),
    created_at  TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
                             ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_product_id (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_device (
    device_key       BIGINT       AUTO_INCREMENT PRIMARY KEY,
    device_id        INT,
    device_type      VARCHAR(255),
    manufacturer     VARCHAR(255),
    model_number     VARCHAR(255),
    install_date     TIMESTAMP    NULL,
    firmware_version INT,
    status           ENUM('0','1'),
    created_at       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
                                  ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_device_id (device_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_pass (
    pass_key             BIGINT        AUTO_INCREMENT PRIMARY KEY,
    pass_id              BIGINT,
    facility_key         BIGINT,
    partner_account_key  BIGINT,
    pass_status          VARCHAR(50),
    start_datetime       TIMESTAMP     NULL,
    end_datetime         TIMESTAMP     NULL,
    pass_name            VARCHAR(255),
    pass_type            VARCHAR(255),
    uses                 VARCHAR(50),
    price                DECIMAL(10,2),
    created_at           DATETIME,
    updated_at           DATETIME,
    effective_start_date DATETIME,
    effective_end_date   DATETIME,
    is_current           BOOLEAN,
    record_hash          CHAR(32),
    UNIQUE KEY uk_pass_id         (pass_id),
    INDEX idx_dim_pass_facility   (facility_key),
    INDEX idx_dim_pass_partner    (partner_account_key),
    INDEX idx_dim_pass_is_current (pass_id, is_current)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_event (
    event_key         BIGINT        AUTO_INCREMENT PRIMARY KEY,
    event_id          INT,
    facility_id       INT,
    event_name        VARCHAR(255),
    event_description TEXT,
    event_category    VARCHAR(255),
    event_start_date  DATE,
    event_end_date    DATE,
    event_start_time  DATETIME      NULL,
    event_end_time    DATETIME      NULL,
    event_rate        DECIMAL(10,2),
    is_active         TINYINT(1),
    created_at        DATETIME,
    updated_at        DATETIME,
    UNIQUE KEY uk_event_facility (event_id, facility_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_permit_plan (
    permit_key             BIGINT        AUTO_INCREMENT PRIMARY KEY,
    permit_id              INT,
    permit_type            VARCHAR(255),
    permit_frequency_unit  VARCHAR(255),
    price                  DECIMAL(10,2),
    max_facilities_allowed INT,
    effective_start_date   DATETIME      NOT NULL,
    effective_end_date     DATETIME      NOT NULL,
    is_current             BOOLEAN       NOT NULL DEFAULT TRUE,
    record_hash            CHAR(32),
    created_at             TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at             TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
                                         ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_permit_id       (permit_id),
    INDEX idx_permit_business_key (permit_id),
    INDEX idx_permit_lookup       (permit_id, is_current)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_reason (
    reason_key       BIGINT        AUTO_INCREMENT PRIMARY KEY,
    reason_id_source INT,
    reason_name      VARCHAR(255),
    penalty_fee      DECIMAL(10,2),
    reason_category  VARCHAR(255),
    created_at       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
                                   ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_reason (reason_name, reason_category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_source_system (
    source_system_key     INT          AUTO_INCREMENT PRIMARY KEY,
    source_ref_id         INT          NOT NULL,
    source_name           VARCHAR(100) NOT NULL,
    api_version           VARCHAR(50),
    is_current            TINYINT      DEFAULT 1,
    reporting_api_version VARCHAR(50),
    created_at            TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
                                       ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_source_ref_id (source_ref_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -----------------------------------------------------------------------------
--  dim_policy
--  Sourced from: SOURCE_DB.business_policy (WHERE deleted_at IS NULL)
--  SCD Type 2 scaffolding: effective_date + is_current
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_policy (
    policy_key          INT          NOT NULL AUTO_INCREMENT,
    policy_id           INT          NOT NULL,
    policy_name         VARCHAR(100) NULL,
    user_type           INT          NULL,
    consumption_channel VARCHAR(11)  NULL,
    discount_type       VARCHAR(11)  NULL,
    discount_value      VARCHAR(11)  NULL,
    validity_start_date DATETIME     NULL,
    validity_end_date   DATETIME     NULL,
    partner_id          INT          NULL,
    created_by          INT          NULL,
    rm_id               INT UNSIGNED NULL,
    status              INT          NULL DEFAULT 0,
    effective_date      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_current          TINYINT(1)   NOT NULL DEFAULT 1,
    created_at          DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
                                              ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (policy_key),
    INDEX idx_policy_id  (policy_id),
    INDEX idx_partner_id (partner_id),
    INDEX idx_is_current (is_current)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Dimension table for Business Policy';

-- =============================================================================
--  FACT TABLES
--  Load order MUST be:
--    1. fact_reservation
--    2. fact_permit_subscription
--    3. fact_passes
--    4. fact_parking_session
--    5. fact_validation_redemption
--    6. fact_payment
--    7. fact_payment_sweep_transactions
-- =============================================================================

-- -----------------------------------------------------------------------------
--  fact_reservation  (unchanged)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_reservation (
    reservation_key       BIGINT        NOT NULL AUTO_INCREMENT,
    source_reservation_id BIGINT        NULL,
    source_transaction_id BIGINT        NULL,
    facility_key          BIGINT        NULL,
    event_key             BIGINT        NULL,
    partner_account_key   BIGINT        NULL,
    parker_key            BIGINT        NULL,
    vehicle_key           BIGINT        NULL,
    rateplan_key          BIGINT        NULL,
    created_ts            TIMESTAMP     NULL,
    start_ts              TIMESTAMP     NULL,
    end_ts                TIMESTAMP     NULL,
    status                VARCHAR(50)   NULL,
    promo_key             BIGINT        NULL,
    booking_source        VARCHAR(250)  NULL,
    license_plate         VARCHAR(10)   NULL,
    booking_id            VARCHAR(250)  NULL,
    policy_key            BIGINT        NULL
        COMMENT 'FK to dim_policy; NULL in initial load — reserved for future mapping',
    created_at            TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
                                        ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (reservation_key),
    UNIQUE KEY uk_source_reservation_id    (source_reservation_id),
    UNIQUE KEY uk_source_transaction_id    (source_transaction_id),
    KEY idx_fact_reservation_facility_key  (facility_key),
    KEY idx_fact_reservation_event_key     (event_key),
    KEY idx_fact_reservation_partner_key   (partner_account_key),
    KEY idx_fact_reservation_parker_key    (parker_key),
    KEY idx_fact_reservation_vehicle_key   (vehicle_key),
    KEY idx_fact_reservation_rate_plan_key (rateplan_key),
    KEY idx_fact_reservation_promo_key     (promo_key),
    KEY idx_fact_reservation_booking_id    (booking_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -----------------------------------------------------------------------------
--  fact_permit_subscription
--  ✅ DROPPED : facility_group_key, billed_amount, paid_amount,
--               balance, spaces_entitled
--  ✅ ADDED   : status_date         TIMESTAMP NULL  <- permit_requests.cancelled_at
--  ✅ ADDED   : partner_account_key BIGINT NULL     <- via dim_partner_account
--  ✅ ADDED   : permit_plan_key     BIGINT NULL     <- via dim_permit_plan
--               (permit_requests.permit_rate_id -> permit_rates
--                -> permit_rate_description_id -> permit_rate_descriptions.id)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_permit_subscription (
    permit_subscription_key BIGINT        AUTO_INCREMENT PRIMARY KEY,
    parker_key              BIGINT        NULL,
    facility_key            BIGINT        NULL,
    product_key             BIGINT        NULL,
    period_start_date_key   DATE          NULL,
    period_end_date_key     DATE          NULL,
    status                  VARCHAR(50)   NULL,
    source_permit_id        BIGINT        NULL,
    status_date             TIMESTAMP     NULL
        COMMENT 'permit_requests.cancelled_at',
    partner_account_key     BIGINT        NULL
        COMMENT 'FK to dim_partner_account via partner_id',
    permit_plan_key         BIGINT        NULL
        COMMENT 'FK to dim_permit_plan via permit_rate_id -> permit_rates -> permit_rate_description_id',
    policy_key              BIGINT        NULL
        COMMENT 'FK to dim_policy; reserved for future mapping',
    created_at              TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
                                          ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_source_permit_id (source_permit_id),
    INDEX idx_parker          (parker_key),
    INDEX idx_facility        (facility_key),
    INDEX idx_product         (product_key),
    INDEX idx_partner_account (partner_account_key),
    INDEX idx_permit_plan     (permit_plan_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -----------------------------------------------------------------------------
--  fact_passes  (unchanged)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_passes (
    pass_subscription_key BIGINT        NOT NULL AUTO_INCREMENT,
    parker_key            BIGINT        NULL,
    pass_key              BIGINT        NULL,
    period_start_date_key INT           NULL,
    period_end_date_key   INT           NULL,
    status_date           TIMESTAMP     NULL,
    source_pass_id        VARCHAR(45)   NULL,
    source_user_pass_id   BIGINT        NULL,
    policy_key            BIGINT        NULL
        COMMENT 'FK to dim_policy; NULL in initial load — reserved for future mapping',
    created_at            TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
                                        ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (pass_subscription_key),
    UNIQUE KEY uk_source_user_pass_id         (source_user_pass_id),
    KEY idx_fact_passes_parker_key            (parker_key),
    KEY idx_fact_passes_pass_key              (pass_key),
    KEY idx_fact_passes_period_start_date_key (period_start_date_key),
    KEY idx_fact_passes_period_end_date_key   (period_end_date_key),
    KEY idx_fact_passes_source_pass_id        (source_pass_id),
    KEY idx_fact_passes_source_user_pass_id   (source_user_pass_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -----------------------------------------------------------------------------
--  fact_parking_session
--  ✅ ADDED : stop_parking_time DATETIME NULL
--             Mapped from tickets.stop_parking_time (SOURCE 1 only).
--             NULL for overstay_tickets and ticket_extends sources.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_parking_session (
    canonical_session_key   BIGINT        AUTO_INCREMENT PRIMARY KEY,
    source_id               BIGINT        NOT NULL
        COMMENT 'Raw PK from tickets / overstay_tickets / ticket_extends',
    extension_overstay_flag TINYINT       NOT NULL DEFAULT 0
        COMMENT '0=tickets  1=overstay_tickets  2=ticket_extends',
    facility_key            BIGINT        NULL,
    vehicle_key             BIGINT        NULL,
    parker_key              BIGINT        NULL,
    partner_account_key     BIGINT        NULL,
    rate_plan_key           BIGINT        NULL,
    promo_code_key          BIGINT        NULL,
    entry_date_key          INT           NULL,
    exit_date_key           INT           NULL,
    entry_time_key          INT           NULL,
    exit_time_key           INT           NULL,
    reservation_key         BIGINT        NULL,
    event_key               BIGINT        NULL,
    permit_subscription_key BIGINT        NULL,
    pass_subscription_key   BIGINT        NULL,
    duration_hours          DECIMAL(10,2) NULL,
    reserv_permit_pass_flag TINYINT       NOT NULL DEFAULT 0,
    entitlement_flag        TINYINT(1)    NOT NULL DEFAULT 0,
    validation_applied_flag TINYINT(1)    NOT NULL DEFAULT 0,
    session_status          VARCHAR(20)   NULL,
    session_source_type_key INT           NULL,
    session_quality_score   DECIMAL(5,2)  NULL,
    ticket_number           VARCHAR(255)  NULL,
    lpr_entry_event_id      VARCHAR(100)  NULL,
    lpr_exit_event_id       VARCHAR(100)  NULL,
    session_build_version   VARCHAR(50)   NULL,
    license_plate           VARCHAR(45)   NULL,
    validation_code         VARCHAR(255)  NULL,
    attendant_user_id       INT           NULL,
    policy_key              BIGINT        NULL
        COMMENT 'FK to dim_policy; mapped from tickets.policy_id (NULL for overstay/extends)',
    validation_refund_flag  TINYINT(1)    NOT NULL DEFAULT 0
        COMMENT '1 = ticket has at least one row in validation_refunds; 0 = none',
    stop_parking_time       DATETIME      NULL
        COMMENT 'tickets.stop_parking_time; NULL for overstay_tickets and ticket_extends',
    created_at              TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
                                          ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_source_session     (extension_overstay_flag, source_id),
    INDEX idx_fps_ticket_number      (ticket_number),
    INDEX idx_fps_facility           (facility_key),
    INDEX idx_fps_vehicle            (vehicle_key),
    INDEX idx_fps_reservation        (reservation_key),
    INDEX idx_fps_event              (event_key),
    INDEX idx_fps_permit_sub         (permit_subscription_key),
    INDEX idx_fps_pass_sub           (pass_subscription_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -----------------------------------------------------------------------------
--  fact_validation_redemption  (unchanged)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_validation_redemption (
    redemption_key        BIGINT        AUTO_INCREMENT PRIMARY KEY,
    redemption_ts_utc     TIMESTAMP     NULL,
    date_key              INT           NULL,
    facility_key          BIGINT        NULL,
    promo_key             BIGINT        NULL,
    canonical_session_key BIGINT        NULL,
    reservation_key       BIGINT        NULL,
    redemption_amount     DECIMAL(12,2) DEFAULT 0,
    approved_flag         TINYINT       NULL,
    rule_version          VARCHAR(50)   NULL,
    source_ticket_id      BIGINT        NOT NULL,
    created_at            TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
                                        ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_ticket   (source_ticket_id),
    INDEX idx_date         (date_key),
    INDEX idx_facility     (facility_key),
    INDEX idx_promo        (promo_key),
    INDEX idx_session      (canonical_session_key),
    INDEX idx_reservation  (reservation_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -----------------------------------------------------------------------------
--  fact_payment
--  ✅ CHANGED : payment_ts_utc — now NULL when source_transaction_id
--               (anet_transaction_id) is NULL.
--  ✅ CHANGED : processing_fees — now maps to tickets.processing_fee ONLY.
--               For non-ticket sources the legacy combined (proc+add) value
--               is still stored here. additional_fees holds the split value.
--  ✅ ADDED   : additional_fees        DECIMAL(8,2)  NULL
--               tickets.additional_fee (SOURCE 1 only; NULL for all other sources)
--  ✅ ADDED   : release_surcharge_fee  DECIMAL(10,2) NULL
--               tickets.release_surcharge_fee; NULL for non-ticket sources
--  ✅ ADDED   : release_tax_fee        DECIMAL(10,2) NULL
--               tickets.release_tax_fee; NULL for non-ticket sources
--  ✅ ADDED   : release_additional_fee DECIMAL(10,2) NULL
--               tickets.release_additional_fee; NULL for non-ticket sources
--  ✅ ADDED   : refund_status          VARCHAR(255)  NULL
--               tickets.refund_status; NULL for non-ticket sources
--  ✅ ADDED   : refund_release_status  TINYINT(1)    NULL
--               tickets.refund_release_status; NULL for non-ticket sources
--  ✅ ADDED   : refund_transaction_id  VARCHAR(255)  NULL
--               tickets.refund_transaction_id; NULL for non-ticket sources
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_payment (
    payment_key             BIGINT        AUTO_INCREMENT PRIMARY KEY,
    source_transaction_id   BIGINT        NULL,
    payment_ts_utc          TIMESTAMP     NULL
        COMMENT 'anet_transactions.created_at; NULL when source_transaction_id (anet_transaction_id) is NULL',
    date_key                INT           NULL,
    facility_key            BIGINT        NULL,
    payment_time_key        INT           NULL,
    payment_method_key      BIGINT        NULL,
    processor_key           BIGINT        NULL,
    canonical_session_key   BIGINT        NULL,
    reservation_key         BIGINT        NULL,
    event_key               BIGINT        NULL,
    permit_subscription_key BIGINT        NULL,
    pass_subscription_key   BIGINT        NULL,
    transaction_type        VARCHAR(50)   NULL,
    amount                  DECIMAL(12,2) NULL,
    approved_flag           BOOLEAN       NULL,
    card_type               VARCHAR(50)   NULL,
    processor_txn_id        VARCHAR(255)  NULL,
    reason_key              BIGINT        NULL,
    sales_tax               DECIMAL(12,2) NULL,
    transaction_date        TIMESTAMP     NULL,
    cc_refund_amount        DECIMAL(12,2) NULL,
    city_surcharge          DECIMAL(12,2) NULL,
    posted_gross_amount     DECIMAL(12,2) NULL,
    discount_amount         DECIMAL(12,2) NULL,
    base_parking_amount     DECIMAL(12,2) NULL,
    validate_amount         DECIMAL(12,2) NULL,
    void_amount             DECIMAL(10,2) NULL,
    release_parking_amount  DECIMAL(10,2) NULL,
    processing_fees         DECIMAL(8,2)  NULL
        COMMENT 'tickets.processing_fee only; non-ticket sources retain combined proc+add',
    oversize_fees           DECIMAL(8,2)  NULL,
    additional_fees         DECIMAL(8,2)  NULL
        COMMENT 'tickets.additional_fee (SOURCE 1 only); NULL for all other sources',
    net_parking_amount      DECIMAL(12,2) NULL,
    refund_date             TIMESTAMP     NULL,
    permit_prorate          DECIMAL(12,2) NULL,
    is_offline_payment      TINYINT(1)    NOT NULL DEFAULT 0,
    tax_exempt_flag         ENUM('0','1','2','3','4','5','6','7','8','9') NULL
        COMMENT 'Mapped from tickets.paid_type; NULL for non-ticket sources',
    sales_tax_exemption     DECIMAL(12,2) NULL DEFAULT 0
        COMMENT 'Tax exemption amount; hardcoded 0 in this load',
    sales_tax_collected     DECIMAL(12,2) NULL DEFAULT 0
        COMMENT 'Tax collected amount; hardcoded 0 in this load',
    validate_refund_amount  DECIMAL(10,2) NULL
        COMMENT 'validation_refunds.total for this ticket; NULL if no refund or non-ticket source',
    vr_anet_trans_id        INT           NULL
        COMMENT 'validation_refunds.anet_transaction_id; NULL if no refund or non-ticket source',
    vr_refund_status        ENUM('PENDING','FAILED','REFUNDED') NULL
        COMMENT 'validation_refunds.transaction_status; NULL if no refund or non-ticket source',
    release_surcharge_fee   DECIMAL(10,2) NULL
        COMMENT 'tickets.release_surcharge_fee; NULL for non-ticket sources',
    release_tax_fee         DECIMAL(10,2) NULL
        COMMENT 'tickets.release_tax_fee; NULL for non-ticket sources',
    release_additional_fee  DECIMAL(10,2) NULL
        COMMENT 'tickets.release_additional_fee; NULL for non-ticket sources',
    refund_status           VARCHAR(255)  NULL
        COMMENT 'tickets.refund_status; NULL for non-ticket sources',
    refund_release_status   TINYINT(1)    NULL
        COMMENT 'tickets.refund_release_status; NULL for non-ticket sources',
    refund_transaction_id   VARCHAR(255)  NULL
        COMMENT 'tickets.refund_transaction_id; NULL for non-ticket sources',
    created_at              TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
                                          ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_source_transaction_id (source_transaction_id),
    INDEX idx_date_key                  (date_key),
    INDEX idx_facility_key              (facility_key),
    INDEX idx_session_key               (canonical_session_key),
    INDEX idx_reservation_key           (reservation_key),
    INDEX idx_pass_sub_key              (pass_subscription_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- -----------------------------------------------------------------------------
--  fact_payment_sweep_transactions  (unchanged)
--  Sourced from: SOURCE_DB.payment_sweep_transactions
--  Resolution: pst.transaction_id -> fact_payment.processor_txn_id
--              -> fact_payment.payment_key / canonical_session_key / payment_method_key
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_payment_sweep_transactions (
    payment_sweep_key            BIGINT        AUTO_INCREMENT PRIMARY KEY,
    source_sweep_id              BIGINT        NOT NULL
        COMMENT 'payment_sweep_transactions.id from source',
    facility_key                 BIGINT        NULL,
    start_date_key               INT           NULL
        COMMENT 'dim_date key for transaction_at date',
    end_date_key                 INT           NULL
        COMMENT 'dim_date key for funded_at date',
    start_time_key               INT           NULL
        COMMENT 'dim_time key for transaction_at time',
    end_time_key                 INT           NULL
        COMMENT 'dim_time key for funded_at time',
    partner_account_key          BIGINT        NULL,
    canonical_session_key        BIGINT        NULL,
    payment_method_key           BIGINT        NULL,
    payment_key                  BIGINT        NULL,
    cc_base_fees                 DECIMAL(12,2) NULL,
    cc_variable_fees             DECIMAL(12,2) NULL,
    pe_base_transaction_fees     DECIMAL(12,2) NULL,
    pe_variable_transaction_fees DECIMAL(12,2) NULL,
    partner_base_fees            DECIMAL(12,2) NULL,
    partner_variable_fees        DECIMAL(12,2) NULL,
    account_number               VARCHAR(50)   NULL,
    sweep_batch                  VARCHAR(50)   NULL,
    service_type                 VARCHAR(255)  NULL,
    created_at                   TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    updated_at                   TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
                                               ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_source_sweep_id  (source_sweep_id),
    INDEX idx_fpst_facility        (facility_key),
    INDEX idx_fpst_partner         (partner_account_key),
    INDEX idx_fpst_session         (canonical_session_key),
    INDEX idx_fpst_payment         (payment_key),
    INDEX idx_fpst_start_date      (start_date_key),
    INDEX idx_fpst_sweep_batch     (sweep_batch)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Fact table for payment sweep transactions (CC settlement data)';

-- =============================================================================
--  END OF SCHEMA
-- =============================================================================