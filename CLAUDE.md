## General Rules

When making file edits (especially config files, profiles, or CVs), NEVER overwrite or remove existing content without explicitly asking the user first. Always show a diff or summary of changes before applying.

## Git Workflow

After running `git commit`, ALWAYS run `git push` immediately unless the user explicitly says not to. Confirm the push was successful before reporting the task as done.

At the end of every session, commit and push all uncommitted changes unless the user explicitly says not to.

## Bash / Shell

Avoid interactive CLI tools in Bash. Always use non-interactive flags (e.g., `--yes`, `--default`, `-y`) or pipe expected input. If a tool absolutely requires an interactive terminal, tell the user immediately instead of retrying.
## Architecture & Gotchas

- **Layout:** `app.py` (Streamlit UI, main interface) and `main.py` (CLI) sit on `src/`: `fetcher` (all Tiingo + SEC EDGAR I/O), `macro` (sector ETFs, RSS news), `analyzer` (ratios/TTM/YoY + `compute_rating`), `technicals` (price stats, reverse DCF), `db` (SQLite `finance.db`, used by the CLI), `display`/`exporter` (CLI output). Keep `analyzer`, `technicals`, `utils` and most of `macro` pure (no I/O) so they stay offline-testable.
- **Data sources:** prices from Tiingo (`TIINGO_API_KEY`: env → `.env` → `st.secrets`); statements from EDGAR XBRL (US-GAAP + IFRS). yfinance/yahooquery/FMP were all abandoned over rate limits — don't reintroduce them.
- **Web app cache order (`_get_ticker`):** `st.session_state` → `data/<TICKER>.json` (committed, for Cloud cold starts) → live `fetch_only()` (never written to disk). Tickers without fundamentals (`UnknownTickerError`/`NoFundamentalsError`) fall back to the price-only fund view. Refresh with `python prefetch_data.py`.
- **Ticker validation:** `^[A-Z]{1,10}([.-][A-Z]{1,4})?$` everywhere — tickers reach API URLs and export filenames.
- **Rating:** 100-pt model, sector-relative thresholds in `_VAL_PROFILES`; financials skip EV/EBITDA and FCF yield. Missing inputs rescale, never penalize.
- **Streamlit Cloud runs latest Streamlit on Python 3.13; local is 3.9 / 1.50.** CSS/DOM differs — prefer `.streamlit/config.toml` theming and element-agnostic selectors (`[role="tab"]`). Give widgets per-ticker keys (avoids `StreamlitDuplicateElementId`).
- **Tiingo key is shared with the deployed app** — heavy local fetching burns Cloud's hourly quota. Prefer tests (`python3 -m pytest`, all offline) or cached `data/`.
- Longer human-oriented notes live in the Obsidian vault: `~/Documents/Brain/Second Brain/Fin-project/` (may lag the code; code wins).
