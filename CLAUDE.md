## General Rules

When making file edits (especially config files, profiles, or CVs), NEVER overwrite or remove existing content without explicitly asking the user first. Always show a diff or summary of changes before applying.

## Git Workflow

After running `git commit`, ALWAYS run `git push` immediately unless the user explicitly says not to. Confirm the push was successful before reporting the task as done.

## Bash / Shell

Avoid interactive CLI tools in Bash. Always use non-interactive flags (e.g., `--yes`, `--default`, `-y`) or pipe expected input. If a tool absolutely requires an interactive terminal, tell the user immediately instead of retrying.