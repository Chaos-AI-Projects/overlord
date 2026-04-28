---
description: Merge upstream template changes from origin/ into working copies
---

Merge upstream changes from the `origin/` directory into your working copies.

When `overlord init` updates templates, it writes the latest versions to `origin/` but skips
locally modified working copies. This skill helps you incorporate those upstream changes.

## Steps

1. **Find diverged files** — compare each file under `origin/` with its working copy:

```bash
cd "$VAULT_DIR"
find origin -type f | while read origin_file; do
  working_file="${origin_file#origin/}"
  if [ -f "$working_file" ]; then
    if ! diff -q "$origin_file" "$working_file" > /dev/null 2>&1; then
      echo "DIVERGED: $working_file"
    fi
  else
    echo "MISSING: $working_file"
  fi
done
```

2. **Review each diverged file** — for every file reported as DIVERGED:
   - Read both `origin/<path>` (the upstream version) and `<path>` (the working copy)
   - Understand what changed upstream vs what was customized locally
   - Merge the changes: preserve local customizations while incorporating upstream additions

3. **Copy missing files** — for files reported as MISSING, simply copy from `origin/`:

```bash
cp "origin/<path>" "<path>"
```

4. **Commit the merge** — after incorporating all changes:

```bash
git add -A
git commit -m "merge upstream template changes from origin/"
```

## Important

- **Do not blindly overwrite** working copies — always review diffs and preserve local customizations.
- The `origin/` directory is managed by `overlord init` and should not be edited manually.
- After merging, running `overlord init` again should show no skipped files.
- If a file in origin/ has not changed since the last merge, there is nothing to do for that file.
