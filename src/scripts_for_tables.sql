--создание схемы ds
CREATE SCHEMA IF NOT EXISTS ds;
--создание таблиц для схемы ds
CREATE TABLE IF NOT EXISTS DS.FT_BALANCE_F (
    on_date DATE NOT NULL,
    account_rk INTEGER NOT NULL,
    currency_rk INTEGER,
    balance_out FLOAT
);
CREATE TABLE IF NOT EXISTS DS.FT_POSTING_F (
    oper_date DATE NOT NULL,
    credit_account_rk INTEGER NOT NULL,
    debet_account_rk INTEGER NOT NULL,
    credit_amount FLOAT,
    debet_amount FLOAT
);
CREATE TABLE IF NOT EXISTS DS.MD_ACCOUNT_D (
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE NOT NULL,
    account_rk INTEGER NOT NULL,
    account_number VARCHAR(20) NOT NULL,
    char_type VARCHAR(1) NOT NULL,
    currency_rk INTEGER NOT NULL,
    currency_code VARCHAR(3) NOT NULL
);
CREATE TABLE IF NOT EXISTS DS.MD_CURRENCY_D (
    currency_rk INTEGER NOT NULL,
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    currency_code VARCHAR(3),
    code_iso_char VARCHAR(3)
);
CREATE TABLE IF NOT EXISTS DS.MD_EXCHANGE_RATE_D (
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    currency_rk INTEGER NOT NULL,
    reduced_cource FLOAT,
    code_iso_num VARCHAR(3)
);
CREATE TABLE IF NOT EXISTS DS.MD_LEDGER_ACCOUNT_S (
    chapter CHAR(1),
    chapter_name VARCHAR(16),
    section_number INTEGER,
    section_name VARCHAR(22),
    subsection_name VARCHAR(21),
    ledger1_account INTEGER,
    ledger1_account_name VARCHAR(47),
    ledger_account INTEGER,
    ledger_account_name VARCHAR(153),
    characteristic CHAR(1),
    is_resident INTEGER,
    is_reserve INTEGER,
    is_reserved INTEGER,
    is_loan INTEGER,
    is_reserved_assets INTEGER,
    is_overdue INTEGER,
    is_interest INTEGER,
    pair_account VARCHAR(5),
    start_date DATE NOT NULL,
    end_date DATE,
    is_rub_only INTEGER,
    min_term INTEGER,
    min_term_measure VARCHAR(1),
    max_term VARCHAR(1),
    max_term_measure VARCHAR(1),
    ledger_acc_full_name_translit VARCHAR(1),
    is_revaluation VARCHAR(1),
	is_correct VARCHAR(1)
);
--создание схемы logs
CREATE SCHEMA IF NOT EXISTS logs;
--создание таблицы для логов 
 CREATE TABLE IF NOT EXISTS logs.logs (
    log_id SERIAL PRIMARY KEY,
    log_time TIMESTAMP,
    name_of_table VARCHAR(30),
    log_data JSONB,
    status VARCHAR(10)
);
