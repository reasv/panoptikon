# Shared helpers for scripts/git-hooks/* (sourced, not executed).
# shellcheck shell=bash

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

have_nix() {
  have_cmd nix
}

py() {
  if have_cmd python3; then
    python3 "$@"
  elif have_nix; then
    nix run nixpkgs#python3 -- "$@"
  else
    echo "error: need python3 (or nix) on PATH for UI pin sync" >&2
    return 1
  fi
}

PIN_PATH="contrib/package/nix/panoptikon/ui-pin.json"
SYNC_SCRIPT="scripts/sync-nix-ui-pin.py"

# ui gitlink SHA: "index" (what the next commit will record) or "HEAD".
# Use rev-parse; `git ls-tree :0` is not a valid tree-ish.
ui_gitlink_rev() {
  local which="${1:-index}"
  case "$which" in
    :0 | index | INDEX)
      git rev-parse :ui 2>/dev/null || git rev-parse :0:ui 2>/dev/null || true
      ;;
    HEAD | head)
      git rev-parse HEAD:ui 2>/dev/null || true
      ;;
    *)
      git rev-parse "${which}:ui" 2>/dev/null || true
      ;;
  esac
}

# Extract "rev" from a pin JSON blob on stdin.
_pin_rev_from_json_stdin() {
  if have_cmd python3; then
    python3 -c 'import json,sys; print(json.load(sys.stdin).get("rev",""))' 2>/dev/null || true
  else
    sed -n 's/.*"rev"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1
  fi
}

# Pin rev as recorded in the index (staged), HEAD, or working tree file.
pin_rev() {
  local which="${1:-index}"
  case "$which" in
    :0 | index | INDEX)
      if git cat-file -e ":${PIN_PATH}" 2>/dev/null; then
        git show ":${PIN_PATH}" | _pin_rev_from_json_stdin
      elif [[ -f $PIN_PATH ]]; then
        # New file not yet in the index object store the same way — use worktree.
        _pin_rev_from_json_stdin <"$PIN_PATH"
      else
        echo ""
      fi
      ;;
    HEAD | head)
      if git cat-file -e "HEAD:${PIN_PATH}" 2>/dev/null; then
        git show "HEAD:${PIN_PATH}" | _pin_rev_from_json_stdin
      else
        echo ""
      fi
      ;;
    worktree | file)
      if [[ -f $PIN_PATH ]]; then
        _pin_rev_from_json_stdin <"$PIN_PATH"
      else
        echo ""
      fi
      ;;
    *)
      echo ""
      ;;
  esac
}

# True if pin rev matches ui gitlink for index or HEAD (rev only; not NAR hash).
pin_rev_matches_gitlink() {
  local which="${1:-index}"
  local gl pin
  gl=$(ui_gitlink_rev "$which")
  pin=$(pin_rev "$which")
  [[ -n $gl && -n $pin && $gl == "$pin" ]]
}
