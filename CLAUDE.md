# X Trader - Project Instructions

## Tech Stack
- Backend: Python/FastAPI + TimescaleDB + Redis
- Frontend: React/Vite
- Monorepo: `core/` (pure logic), `app/` (live system), `backtest/` (backtesting)
- Tests: pytest via `.venv/bin/pytest`

## Collaboration Principles
- Every feature should be planned before coding — confirm the approach first, then implement
- When there are multiple approaches, list the options with trade-offs and let me decide
- Communicate in Chinese, keep technical terms in English
- Always read existing code before making changes — understand context first
- If my idea is too big, suggest a smarter MVP scope
- Hit a problem? Tell me the options instead of just picking one
- Be honest about limitations — I'd rather adjust expectations than be disappointed

## Code Standards
- Keep changes minimal and focused — don't refactor surrounding code unless asked
- Test before moving on to the next step
- Handle edge cases and errors gracefully
- No security vulnerabilities (injection, XSS, etc.)
