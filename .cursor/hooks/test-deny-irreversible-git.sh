#!/usr/bin/env bash
# Static-stdin self-test for deny-irreversible-git.sh; never runs git itself.
# The cases live in this file because the hook inspects the whole command line:
# typing them inline would make the hook deny the test command itself.
set -uo pipefail
hook="$(dirname "$0")/deny-irreversible-git.sh"
failures=0

check()
{
    local expected="$1" command="$2" actual
    actual=$(jq -cn --arg c "$command" '{command: $c}' | bash "$hook" | jq -r '.permission')
    if [[ "$actual" == "$expected" ]]; then
        printf 'ok    %-5s  %s\n' "$expected" "$command"
    else
        printf 'FAIL  %-5s  %s  (got %s)\n' "$expected" "$command" "$actual"
        failures=$((failures + 1))
    fi
}

check allow 'git status'
check allow 'git commit -F tmp/msg.txt'
check allow 'git cherry-pick --no-commit abc'
check allow 'echo git pushes'
check deny  'git push origin HEAD'
check deny  'cd repo && git push'
check deny  'git -C /repo push'
check deny  'git -c user.name=x push'
check deny  'git --git-dir=/repo/.git push'
check deny  'git push --force-with-lease'
check deny  'git rebase -i HEAD~2'
check deny  'git -C /repo rebase main'

no_jq=$(echo '{"command":"git status"}' | env -i PATH=/nonexistent /usr/bin/bash "$hook" | sed -n 's/.*"permission":"\([a-z]*\)".*/\1/p')
if [[ "$no_jq" == "deny" ]]; then
    echo 'ok    deny   (jq missing)'
else
    echo "FAIL  deny   (jq missing, got $no_jq)"
    failures=$((failures + 1))
fi

exit "$failures"
