# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///
"""
Copies files that differ between the current branch (committed) and main
into an 'extensions' folder, retaining directory structure.
"""

import shutil
import subprocess
from pathlib import Path

DEST = Path("extensions/skyportal")
EXCLUDED_FILES = {
    "copy_diff_to_extensions.py",
    "package.json",
    "uv.lock",
    "pyproject.toml",
    "app_server.py.gitignore",
}


def get_diffed_files():
    # Compare committed state of current branch vs main (merge-base diff)
    result = subprocess.run(
        ["git", "diff", "--name-only", "--diff-filter=ACMRT", "main...HEAD"],
        capture_output=True,
        text=True,
        check=True,
    )
    files = [
        f for f in result.stdout.splitlines() if f.strip() and f not in EXCLUDED_FILES
    ]
    return files


def copy_files(files):
    repo_root = Path(
        subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    )

    copied, skipped = [], []
    for rel_path in files:
        src = repo_root / rel_path
        if not src.exists():
            skipped.append(rel_path)
            continue
        dest = DEST / rel_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        copied.append(rel_path)

    return copied, skipped


def main():
    files = get_diffed_files()
    if not files:
        print("No diffed files found between HEAD and main.")
        return

    if DEST.exists():
        print(f"Removing existing destination: '{DEST}/'")
        shutil.rmtree(DEST)

    print(f"Found {len(files)} diffed file(s). Copying to '{DEST}/'...\n")
    copied, skipped = copy_files(files)

    for f in copied:
        print(f"  [copied]  {f}")
    for f in skipped:
        print(f"  [skipped] {f}  (not found on disk — likely deleted)")

    print(f"\nDone. {len(copied)} file(s) copied, {len(skipped)} skipped.")


if __name__ == "__main__":
    main()
