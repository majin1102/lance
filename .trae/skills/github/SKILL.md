---
name: "github"
description: "GitHub integration using gh CLI for commits, PRs, reviews, and more. Invoke when user asks to commit code, create PR, get reviews, or interact with GitHub."
---

# GitHub Skill

This skill provides GitHub integration using the GitHub CLI (gh). It supports:

## Features

1. **Git Operations**:
   - Check git status
   - Stage changes (git add)
   - Commit changes
   - Push to remote
   - Create branches
   - Switch branches

2. **Pull Request Operations**:
   - Create pull requests
   - List pull requests
   - View pull request details
   - Get PR reviews and comments
   - Merge pull requests

3. **Repository Operations**:
   - View repository info
   - List issues
   - Create issues

## Usage

When the user asks to interact with GitHub, this skill will use the `gh` CLI to perform the requested operations.

## Common Commands

- Check status: `gh repo view` or `git status`
- Create PR: `gh pr create --title "Title" --body "Description"`
- List PRs: `gh pr list`
- View PR: `gh pr view <pr-number>`
- Get PR reviews: `gh pr review <pr-number>`
- Merge PR: `gh pr merge <pr-number>`

## Prerequisites

- GitHub CLI (gh) must be installed and authenticated
- Must be in a git repository
