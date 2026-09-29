#!/bin/sh
# Issue one invitation per host pod from the running founder and install
# them as the secret the host StatefulSets mount (24 §24). Run where
# kubectl reaches the cluster, once focal-founder-0 is Ready. Invitations
# are one-use and expire after an hour; rerun for a pod that never joined.
set -eu
NAMESPACE="focal"
SECRET="focal-invitations"
HOSTS="focal-b-0 focal-c-0"
FILES=""
for host in $HOSTS; do
  kubectl -n "$NAMESPACE" exec focal-founder-0 -c focal -- /focal --data-dir /var/lib/focal cluster invite --node "$host" --output - > "$host.invite"
  FILES="$FILES --from-file=$host.invite=$host.invite"
done
kubectl -n "$NAMESPACE" delete secret "$SECRET" --ignore-not-found
# shellcheck disable=SC2086
kubectl -n "$NAMESPACE" create secret generic "$SECRET" $FILES
rm -f -- *.invite
