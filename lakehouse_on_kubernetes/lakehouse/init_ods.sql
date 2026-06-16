-- ODS database for the Korean insurance company simulation.
-- PostgreSQL mimics an on-premise Oracle/MSSQL ODS that ADF would read from.

CREATE USER ods WITH PASSWORD 'ods';
CREATE DATABASE ods OWNER ods;

\connect ods

-- 고객 (Customer)
CREATE TABLE IF NOT EXISTS customer (
    customer_id     VARCHAR(20) PRIMARY KEY,
    name            VARCHAR(100) NOT NULL,
    rrn_masked      VARCHAR(20) NOT NULL,    -- 주민번호 masked: 123456-*******
    birth_date      DATE,
    gender          CHAR(1),                 -- M / F
    channel         VARCHAR(30),             -- 설계사 | 온라인 | 방카슈랑스 | 다이렉트
    phone_masked    VARCHAR(20),
    email           VARCHAR(100),
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- 계약 (Policy)
CREATE TABLE IF NOT EXISTS policy (
    policy_id       VARCHAR(20) PRIMARY KEY,
    customer_id     VARCHAR(20) REFERENCES customer(customer_id),
    product_code    VARCHAR(20) NOT NULL,    -- LIFE | HEALTH | AUTO | FIRE
    product_name    VARCHAR(100),
    start_date      DATE NOT NULL,
    end_date        DATE,
    premium         NUMERIC(12, 2) NOT NULL,
    status          VARCHAR(20) DEFAULT 'active',  -- active | lapsed | cancelled | matured
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- 청구 (Claims)
CREATE TABLE IF NOT EXISTS claims (
    claim_id        VARCHAR(20) PRIMARY KEY,
    policy_id       VARCHAR(20) REFERENCES policy(policy_id),
    customer_id     VARCHAR(20) REFERENCES customer(customer_id),
    claim_date      DATE NOT NULL,
    claim_amount    NUMERIC(14, 2) NOT NULL,
    settled_amount  NUMERIC(14, 2),
    status          VARCHAR(30) DEFAULT '접수',  -- 접수 | 심사중 | 지급완료 | 부지급
    claim_type      VARCHAR(50),
    processed_at    TIMESTAMP,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- 민원 (VOC — Voice of Customer)
CREATE TABLE IF NOT EXISTS voc (
    voc_id          VARCHAR(20) PRIMARY KEY,
    customer_id     VARCHAR(20) REFERENCES customer(customer_id),
    complaint_type  VARCHAR(50) NOT NULL,   -- 보험료 | 보험금 | 상품 | 서비스 | 기타
    channel         VARCHAR(30),            -- 전화 | 온라인 | 방문 | 우편
    description     TEXT,
    priority        VARCHAR(10) DEFAULT 'normal',  -- high | normal | low
    status          VARCHAR(20) DEFAULT 'open',    -- open | in_progress | resolved | closed
    created_at      TIMESTAMP DEFAULT NOW(),
    resolved_at     TIMESTAMP,
    resolution_note TEXT
);
