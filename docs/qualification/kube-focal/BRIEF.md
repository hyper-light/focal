# kube-focal: a Kubernetes in Rust, built by agents through Focal

Build a minimal, working Kubernetes in Rust as one Cargo workspace in this repository:

- **API types**: `Pod`, `Deployment` and `Node`, each with Kubernetes-style metadata (name,
  namespace, labels, uid, resourceVersion), spec and status, serialized as JSON.
- **Store**: versioned objects with optimistic concurrency (resourceVersion) and watches that
  deliver every change after a given version.
- **API server**: HTTP + JSON: create, get, list, update, delete and watch for pods, deployments
  and nodes, with a binding subresource for pods.
- **Scheduler**: binds each pending pod to a ready node that has room for its CPU and memory
  requests, spreading replicas over nodes.
- **Deployment controller**: keeps each deployment's replicas of pods from its template, scaling
  up and down.
- **Kubelet**: registers its node with capacity, runs the pods bound to it as local processes
  (the container's command and args), reports Running, Succeeded or Failed, restarts per the
  pod's restart policy, and stops pods that are deleted.
- **kubectl**: `apply -f` (JSON or YAML), `get`, `delete` and `scale`.

**Acceptance**: an end-to-end test starts the API server, scheduler, deployment controller and
two kubelets as separate processes, applies a 3-replica deployment running `sleep 30`, sees three
Running pods spread over both nodes, scales it to one, deletes it, and sees every pod gone. It and
every component's own tests pass with `cargo test --workspace`.

The work is coordinated through the Focal ledger: claims for the work, artifacts for what is
delivered (each a commit of this repository), testaments that close the work, and validations
that check it. Nobody outside the ledger assigns work.
