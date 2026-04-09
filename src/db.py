import sqlite3
import os
from contextlib import closing
from datetime import date

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'finance.db')


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with closing(get_conn()) as conn:
        c = conn.cursor()
        c.executescript("""
            CREATE TABLE IF NOT EXISTS companies (
                ticker       TEXT PRIMARY KEY,
                name         TEXT,
                sector       TEXT,
                industry     TEXT,
                currency     TEXT,
                exchange     TEXT,
                last_updated TEXT
            );

            CREATE TABLE IF NOT EXISTS income_statements (
                id                        INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker                    TEXT NOT NULL,
                period                    TEXT NOT NULL,
                revenue                   REAL,
                gross_profit              REAL,
                operating_income          REAL,
                net_income                REAL,
                ebitda                    REAL,
                interest_expense          REAL,
                depreciation_amortization REAL,
                UNIQUE(ticker, period)
            );

            CREATE TABLE IF NOT EXISTS balance_sheets (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker              TEXT NOT NULL,
                period              TEXT NOT NULL,
                total_assets        REAL,
                total_liabilities   REAL,
                equity              REAL,
                cash                REAL,
                total_debt          REAL,
                current_assets      REAL,
                current_liabilities REAL,
                inventory           REAL,
                UNIQUE(ticker, period)
            );

            CREATE TABLE IF NOT EXISTS cash_flows (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker         TEXT NOT NULL,
                period         TEXT NOT NULL,
                operating_cf   REAL,
                investing_cf   REAL,
                financing_cf   REAL,
                capex          REAL,
                free_cash_flow REAL,
                UNIQUE(ticker, period)
            );

            CREATE TABLE IF NOT EXISTS market_data (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker           TEXT NOT NULL,
                fetched_date     TEXT NOT NULL,
                market_cap       REAL,
                enterprise_value REAL,
                shares_outstanding REAL,
                price            REAL,
                pe_trailing      REAL,
                pe_forward       REAL,
                pb_ratio         REAL,
                ev_ebitda_info   REAL,
                ev_revenue_info  REAL,
                dividend_yield   REAL,
                beta             REAL,
                week52_high      REAL,
                week52_low       REAL,
                UNIQUE(ticker, fetched_date)
            );

            CREATE INDEX IF NOT EXISTS idx_income_ticker_period  ON income_statements(ticker, period DESC);
            CREATE INDEX IF NOT EXISTS idx_balance_ticker_period ON balance_sheets(ticker, period DESC);
            CREATE INDEX IF NOT EXISTS idx_cashflow_ticker_period ON cash_flows(ticker, period DESC);
            CREATE INDEX IF NOT EXISTS idx_market_ticker_date    ON market_data(ticker, fetched_date DESC);
        """)

        # Migrate existing DBs that may be missing columns
        for stmt in (
            "ALTER TABLE income_statements ADD COLUMN depreciation_amortization REAL",
            "ALTER TABLE companies ADD COLUMN currency TEXT",
            "ALTER TABLE companies ADD COLUMN exchange TEXT",
        ):
            try:
                c.execute(stmt)
            except sqlite3.OperationalError as e:
                if 'duplicate column name' not in str(e).lower():
                    raise

        conn.commit()


def upsert_company(ticker, info, conn=None):
    _conn = conn or get_conn()
    _conn.execute("""
        INSERT INTO companies (ticker, name, sector, industry, currency, exchange, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ticker) DO UPDATE SET
            name=excluded.name, sector=excluded.sector, industry=excluded.industry,
            currency=excluded.currency, exchange=excluded.exchange,
            last_updated=excluded.last_updated
    """, (
        ticker,
        info.get('longName') or info.get('shortName', ticker),
        info.get('sector', ''),
        info.get('industry', ''),
        info.get('currency', ''),
        info.get('exchange', ''),
        str(date.today()),
    ))
    if conn is None:
        _conn.commit()
        _conn.close()


def upsert_income(ticker, period, row, conn=None):
    _conn = conn or get_conn()
    _conn.execute("""
        INSERT INTO income_statements
            (ticker, period, revenue, gross_profit, operating_income, net_income,
             ebitda, interest_expense, depreciation_amortization)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ticker, period) DO UPDATE SET
            revenue=excluded.revenue, gross_profit=excluded.gross_profit,
            operating_income=excluded.operating_income, net_income=excluded.net_income,
            ebitda=excluded.ebitda, interest_expense=excluded.interest_expense,
            depreciation_amortization=excluded.depreciation_amortization
    """, (ticker, period,
          row.get('revenue'), row.get('gross_profit'), row.get('operating_income'),
          row.get('net_income'), row.get('ebitda'), row.get('interest_expense'),
          row.get('depreciation_amortization')))
    if conn is None:
        _conn.commit()
        _conn.close()


def upsert_balance(ticker, period, row, conn=None):
    _conn = conn or get_conn()
    _conn.execute("""
        INSERT INTO balance_sheets
            (ticker, period, total_assets, total_liabilities, equity, cash, total_debt,
             current_assets, current_liabilities, inventory)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ticker, period) DO UPDATE SET
            total_assets=excluded.total_assets, total_liabilities=excluded.total_liabilities,
            equity=excluded.equity, cash=excluded.cash, total_debt=excluded.total_debt,
            current_assets=excluded.current_assets,
            current_liabilities=excluded.current_liabilities, inventory=excluded.inventory
    """, (ticker, period,
          row.get('total_assets'), row.get('total_liabilities'), row.get('equity'),
          row.get('cash'), row.get('total_debt'), row.get('current_assets'),
          row.get('current_liabilities'), row.get('inventory')))
    if conn is None:
        _conn.commit()
        _conn.close()


def upsert_cashflow(ticker, period, row, conn=None):
    _conn = conn or get_conn()
    _conn.execute("""
        INSERT INTO cash_flows
            (ticker, period, operating_cf, investing_cf, financing_cf, capex, free_cash_flow)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ticker, period) DO UPDATE SET
            operating_cf=excluded.operating_cf, investing_cf=excluded.investing_cf,
            financing_cf=excluded.financing_cf, capex=excluded.capex,
            free_cash_flow=excluded.free_cash_flow
    """, (ticker, period,
          row.get('operating_cf'), row.get('investing_cf'), row.get('financing_cf'),
          row.get('capex'), row.get('free_cash_flow')))
    if conn is None:
        _conn.commit()
        _conn.close()


def upsert_market_data(ticker, row, conn=None):
    _conn = conn or get_conn()
    _conn.execute("""
        INSERT INTO market_data
            (ticker, fetched_date, market_cap, enterprise_value, shares_outstanding, price,
             pe_trailing, pe_forward, pb_ratio, ev_ebitda_info, ev_revenue_info,
             dividend_yield, beta, week52_high, week52_low)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ticker, fetched_date) DO UPDATE SET
            market_cap=excluded.market_cap, enterprise_value=excluded.enterprise_value,
            shares_outstanding=excluded.shares_outstanding, price=excluded.price,
            pe_trailing=excluded.pe_trailing, pe_forward=excluded.pe_forward,
            pb_ratio=excluded.pb_ratio, ev_ebitda_info=excluded.ev_ebitda_info,
            ev_revenue_info=excluded.ev_revenue_info, dividend_yield=excluded.dividend_yield,
            beta=excluded.beta, week52_high=excluded.week52_high, week52_low=excluded.week52_low
    """, (ticker, str(date.today()),
          row.get('market_cap'), row.get('enterprise_value'), row.get('shares_outstanding'),
          row.get('price'), row.get('pe_trailing'), row.get('pe_forward'),
          row.get('pb_ratio'), row.get('ev_ebitda_info'), row.get('ev_revenue_info'),
          row.get('dividend_yield'), row.get('beta'),
          row.get('week52_high'), row.get('week52_low')))
    if conn is None:
        _conn.commit()
        _conn.close()


def fetch_all(ticker):
    with closing(get_conn()) as conn:
        company_row = conn.execute(
            """SELECT ticker, name, sector, industry, currency, exchange, last_updated
               FROM companies WHERE ticker=?""", (ticker,)).fetchone()
        market_row = conn.execute(
            """SELECT market_cap, enterprise_value, shares_outstanding, price,
                      pe_trailing, pe_forward, pb_ratio, ev_ebitda_info, ev_revenue_info,
                      dividend_yield, beta, week52_high, week52_low
               FROM market_data WHERE ticker=? ORDER BY fetched_date DESC LIMIT 1""",
            (ticker,)).fetchone()
        result = {
            'company': dict(company_row) if company_row else {},
            'market':  dict(market_row)  if market_row  else {},
            'income':  [dict(r) for r in conn.execute(
                """SELECT period, revenue, gross_profit, operating_income, net_income,
                          ebitda, interest_expense, depreciation_amortization
                   FROM income_statements WHERE ticker=? ORDER BY period DESC""", (ticker,))],
            'balance': [dict(r) for r in conn.execute(
                """SELECT period, total_assets, total_liabilities, equity, cash, total_debt,
                          current_assets, current_liabilities, inventory
                   FROM balance_sheets WHERE ticker=? ORDER BY period DESC""", (ticker,))],
            'cashflow': [dict(r) for r in conn.execute(
                """SELECT period, operating_cf, investing_cf, financing_cf, capex, free_cash_flow
                   FROM cash_flows WHERE ticker=? ORDER BY period DESC""", (ticker,))],
        }
        return result
