# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository

GitHub: https://github.com/bansipatel/ClaudeCodeTest
Local: `C:\Users\bansi\OneDrive\Desktop\ClaudeCodeTest`

## Git Workflow

**Commit and push to GitHub continuously as work progresses — do not batch everything at the end.** This ensures work is never lost and the repo always reflects the current state.

Rules:
- Commit after every meaningful unit of work (feature added, bug fixed, file created)
- Push immediately after every commit — never leave commits unpushed
- Stage specific files by name, never `git add .` or `git add -A`
- Write clean, imperative commit messages that describe *what changed and why*

```bash
git add <specific files>
git commit -m "short imperative summary

- bullet detail if needed"
git push
```

## Running the App

No build step — open HTML files directly in a browser:

```bash
start tictactoe.html   # Windows
```

## Architecture

This is a static web project: plain HTML files with embedded CSS and JavaScript (no frameworks, no bundler, no dependencies).

### `tictactoe.html`

Self-contained single-file app. All logic lives in one `<script>` block:

- **State**: `board` (9-cell array), `current` (active player), `gameOver`, `vsCPU`, `scores`
- **Render loop**: `render()` rebuilds the board DOM from scratch on every state change
- **CPU logic**: `bestMove()` — win > block > center > corner > random (no minimax)
- **Win detection**: `checkWin()` checks all 8 win combos defined in `WINS`
