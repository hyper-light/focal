{{/*
The node configuration a set ships: the requested policy and the set's zone.
*/}}
{{- define "focal.config" -}}
version: 1
{{- if or .root.Values.region .zone }}
topology:
{{- if .root.Values.region }}
  region: {{ .root.Values.region | quote }}
{{- end }}
{{- if .zone }}
  zone: {{ .zone | quote }}
{{- end }}
{{- end }}
durability:
  survive: {{ .root.Values.survive }}
  max_failures: {{ .root.Values.maxFailures }}
{{- end }}

{{/*
One StatefulSet. Context: root, name, founder (bool), replicas, zone (string or "").
*/}}
{{- define "focal.statefulset" -}}
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: {{ .name }}
  namespace: {{ .root.Release.Namespace }}
  labels:
    app.kubernetes.io/name: focal
    app.kubernetes.io/component: {{ if .founder }}founder{{ else }}host{{ end }}
    app.kubernetes.io/managed-by: {{ .root.Release.Service }}
    focal.dev/set: {{ .name }}
  annotations:
    focal.dev/qualification: unqualified-bootstrap-allocation
spec:
  serviceName: focal
  replicas: {{ .replicas }}
  podManagementPolicy: OrderedReady
  selector:
    matchLabels:
      app.kubernetes.io/name: focal
      focal.dev/set: {{ .name }}
  template:
    metadata:
      labels:
        app.kubernetes.io/name: focal
        app.kubernetes.io/component: {{ if .founder }}founder{{ else }}host{{ end }}
        focal.dev/set: {{ .name }}
    spec:
      terminationGracePeriodSeconds: 45
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        runAsGroup: 65532
        fsGroup: 65532
        seccompProfile:
          type: RuntimeDefault
{{- if .zone }}
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - key: topology.kubernetes.io/zone
                    operator: In
                    values:
                      - {{ .zone | quote }}
{{- end }}
{{- if and (eq .root.Values.survive "node") (gt (int .replicas) 1) }}
      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: kubernetes.io/hostname
          whenUnsatisfiable: DoNotSchedule
          labelSelector:
            matchLabels:
              app.kubernetes.io/name: focal
{{- end }}
      initContainers:
        - name: prepare-volume
          image: {{ .root.Values.image }}
          command: ["/focal"]
          args: ["--data-dir", "/var/lib/focal", "prepare-volume", "--owner", "65532:65532"]
          securityContext:
            runAsUser: 0
            runAsNonRoot: false
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
              add: ["CHOWN", "FOWNER", "DAC_OVERRIDE"]
          volumeMounts:
            - name: data
              mountPath: /var/lib/focal
      containers:
        - name: focal
          image: {{ .root.Values.image }}
          command: ["/focal"]
          args:
            - "--config"
            - "/etc/focal/focal.yaml"
            - "--data-dir"
            - "/var/lib/focal"
            - "start"
            - "--listen"
            - "0.0.0.0:{{ .root.Values.port }}"
            - "--advertise"
            - "$(POD_NAME).focal.$(POD_NAMESPACE).svc.cluster.local:{{ .root.Values.port }}"
{{- if not .founder }}
            - "--invite-file"
            - "/etc/focal/invitations/$(POD_NAME).invite"
{{- end }}
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: POD_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
          ports:
            - name: peer
              containerPort: {{ .root.Values.port }}
              protocol: UDP
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          resources:
{{ toYaml .root.Values.resources | indent 12 }}
          startupProbe:
            exec:
              command: ["/focal", "--data-dir", "/var/lib/focal", "cluster", "node", "probe", "--check", "alive"]
            periodSeconds: 5
            failureThreshold: 60
          livenessProbe:
            exec:
              command: ["/focal", "--data-dir", "/var/lib/focal", "cluster", "node", "probe", "--check", "alive"]
            periodSeconds: 10
            failureThreshold: 6
          readinessProbe:
            exec:
              command: ["/focal", "--data-dir", "/var/lib/focal", "cluster", "node", "probe", "--check", "alive"]
            periodSeconds: 5
            failureThreshold: 3
          volumeMounts:
            - name: data
              mountPath: /var/lib/focal
            - name: config
              mountPath: /etc/focal/focal.yaml
              subPath: focal.yaml
              readOnly: true
{{- if not .founder }}
            - name: invitations
              mountPath: /etc/focal/invitations
              readOnly: true
{{- end }}
      volumes:
        - name: config
          configMap:
            name: focal-config
            items:
              - key: {{ .name }}.yaml
                path: focal.yaml
{{- if not .founder }}
        - name: invitations
          secret:
            secretName: {{ .root.Values.invitationsSecret }}
            defaultMode: 288
{{- end }}
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
{{- if .root.Values.storageClass }}
        storageClassName: {{ .root.Values.storageClass }}
{{- end }}
        resources:
          requests:
            storage: {{ .root.Values.volume }}
{{- end }}
