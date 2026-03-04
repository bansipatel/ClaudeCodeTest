# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository

GitHub: https://github.com/bansipatel/ClaudeCodeTest
Local: `C:\Users\bansi\OneDrive\Desktop\ClaudeCodeTest`

## Git Workflow

Every change must be committed and pushed to GitHub. Use clean, descriptive commit messages. After completing any feature or fix:

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
