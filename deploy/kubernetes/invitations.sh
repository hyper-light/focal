#!/bin/sh
# Issue one invitation per host pod from the running founder and install
# them as the secret the host StatefulSets mount (24 §24). Run where
# kubectl reaches the cluster, once focal-founder-0 is Ready. Invitations
# are one-use and expire after an hour; rerun for a pod that never joined.
set -eu
NAMESPACE="focal"
SECRET="focal-invitations"
n=0
until kubectl -n "$NAMESPACE" exec focal-founder-0 -c focal -- /focal --data-dir /var/lib/focal inspect node --probe authoritative >/dev/null 2>&1; do
  n=$((n + 1))
  if [ "$n" -ge 150 ]; then echo "focal-founder-0 never led its root" >&2; exit 1; fi
  sleep 2
done
HOSTS="focal-b-0 focal-c-0"
FILES=""
for host in $HOSTS; do
  # Its ledger service may answer after the founder leads (a refusal typed
  # unavailable, exit 6, retryable): asked again, bounded.
  n=0
  until kubectl -n "$NAMESPACE" exec focal-founder-0 -c focal -- /focal --data-dir /var/lib/focal invite node --node "$host" --output - > "$host.invite"; do
    n=$((n + 1))
    if [ "$n" -ge 60 ]; then echo "no invitation for $host" >&2; exit 1; fi
    sleep 2
  done
  FILES="$FILES --from-file=$host.invite=$host.invite"
done
kubectl -n "$NAMESPACE" delete secret "$SECRET" --ignore-not-found
# shellcheck disable=SC2086
kubectl -n "$NAMESPACE" create secret generic "$SECRET" $FILES
rm -f -- *.invite
