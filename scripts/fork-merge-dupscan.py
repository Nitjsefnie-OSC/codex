#!/usr/bin/env python3
"""Flag merge duplicates left by rebasing the fork onto a new upstream.

After a conflict rebase, a function that keeps BOTH the fork's and upstream's
version of the same step (double allocation, double setup, double drain,
double lifecycle emission) usually calls something more often than either
parent did. This script compares every non-test `codex-rs/*.rs` function
changed between upstream and the rebased branch against the same function in
the old fork main and in upstream, and prints each call whose count on the
branch exceeds both parents.

Usage (from any directory; reads refs through git, never the working tree):

    python3 scripts/fork-merge-dupscan.py <rebased-branch-ref> \
        [--repo /root/codex] \
        [--old backup/main-before-upstream-rebase-<sha>] \
        [--upstream upstream/main]

Every flag is a lead to triage, not a verdict: a loop body that upstream also
repeats, or a deliberate second call, is a false positive.
"""

import argparse
import collections
import re
import subprocess

IGNORE = set(
    "clone to_string await into unwrap expect map as_ref iter collect push Some Ok Err Arc new "
    "get len is_some is_none as_str lock insert format vec contains unwrap_or join from extend "
    "send debug warn info error trace to_owned into_iter and_then ok_or_else filter ok map_err "
    "as_deref cloned borrow unwrap_or_default is_empty take then_some any all Box pin matches "
    "assert assert_eq with_context context default is_ok is_err unwrap_or_else first last chars".split()
)


def show(repo, ref, path):
    r = subprocess.run(["git", "-C", repo, "show", f"{ref}:{path}"], capture_output=True, text=True)
    return r.stdout if r.returncode == 0 else None


def functions(src):
    out = {}
    for m in re.finditer(r"\n( *)(?:pub(?:\([a-z:]+\))? )?(?:async )?fn (\w+)[^{;]*\{", src):
        ind = m.group(1)
        start = m.end()
        end = src.find("\n" + ind + "}\n", start)
        if end < 0:
            continue
        # Same-name functions in one file (impl blocks, cfg variants) are concatenated.
        out[m.group(2)] = out.get(m.group(2), "") + src[start:end]
    return out


def calls(body):
    body = re.sub(r"//[^\n]*", "", body)
    names = re.findall(r"\b([a-z_][a-z0-9_]{3,})\s*(?:::<[^>]*>)?\(", body)
    return collections.Counter(n for n in names if n not in IGNORE)


def main():
    ap = argparse.ArgumentParser(description="Flag merge duplicates left by rebasing the fork onto a new upstream.")
    ap.add_argument("new", help="the rebased branch ref or SHA, resolved in --repo")
    ap.add_argument("--repo", default="/root/codex")
    ap.add_argument("--old", default="backup/main-before-upstream-rebase-9d05edd", help="old fork main")
    ap.add_argument("--upstream", default="upstream/main")
    args = ap.parse_args()

    diff = subprocess.run(
        ["git", "-C", args.repo, "diff", "--name-only", args.upstream, args.new, "--", "codex-rs/*.rs"],
        capture_output=True,
        text=True,
        check=True,
    )
    for path in diff.stdout.split():
        if re.search(r"(_tests?\.rs|/tests?/|tests\.rs$)", path):
            continue
        new = show(args.repo, args.new, path)
        old = show(args.repo, args.old, path)
        up = show(args.repo, args.upstream, path)
        if not new or not old or not up:
            continue
        fn_new, fn_old, fn_up = functions(new), functions(old), functions(up)
        for name, body in fn_new.items():
            if name not in fn_old and name not in fn_up:
                continue
            cn, co, cu = calls(body), calls(fn_old.get(name, "")), calls(fn_up.get(name, ""))
            hits = [
                (c, cn[c], co[c], cu[c])
                for c in cn
                if cn[c] >= 2 and cn[c] > max(co[c], cu[c]) and (co[c] > 0 or cu[c] > 0)
            ]
            if hits:
                detail = ", ".join(f"{c} new={n} old={o} up={u}" for c, n, o, u in hits)
                print(f"{path}::{name}  {detail}")


if __name__ == "__main__":
    main()
