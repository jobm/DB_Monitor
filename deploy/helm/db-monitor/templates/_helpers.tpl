{{/*
Expand the name of the chart.
*/}}
{{- define "db-monitor.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "db-monitor.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "db-monitor.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "db-monitor.labels" -}}
helm.sh/chart: {{ include "db-monitor.chart" . }}
{{ include "db-monitor.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "db-monitor.selectorLabels" -}}
app.kubernetes.io/name: {{ include "db-monitor.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Container spec shared by the API and consumer Deployments.

Expects a dict with "root" (top-level context) and "role" ("api" or
"consumer"). The role is passed to the process as SERVICE_ROLE so the pod
starts only the tasks it owns.
*/}}
{{- define "db-monitor.container" -}}
{{- $root := .root -}}
{{- $role := .role -}}
- name: {{ $root.Chart.Name }}-{{ $role }}
  image: "{{ $root.Values.image.repository }}:{{ $root.Values.image.tag | default $root.Chart.AppVersion }}"
  imagePullPolicy: {{ $root.Values.image.pullPolicy }}
  securityContext:
    {{- toYaml $root.Values.securityContext | nindent 4 }}
  ports:
    - name: http
      containerPort: 8000
      protocol: TCP
  envFrom:
    - configMapRef:
        name: {{ include "db-monitor.fullname" $root }}-config
    {{- if or (not $root.Values.database.existingSecret) (not $root.Values.auth.existingSecret) (and $root.Values.kafka.tls.enabled $root.Values.kafka.tls.password) }}
    - secretRef:
        name: {{ include "db-monitor.fullname" $root }}-secrets
    {{- end }}
  env:
    - name: SERVICE_ROLE
      value: {{ $role | quote }}
    {{- if $root.Values.database.existingSecret }}
    - name: POSTGRES_URL
      valueFrom:
        secretKeyRef:
          name: {{ $root.Values.database.existingSecret }}
          key: postgres-url
    {{- end }}
    {{- if $root.Values.auth.existingSecret }}
    - name: JWT_SECRET
      valueFrom:
        secretKeyRef:
          name: {{ $root.Values.auth.existingSecret }}
          key: jwt-secret
    {{- end }}
    {{- with $root.Values.extraEnv }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  {{- if or (and $root.Values.retention.archiveBeforeDelete $root.Values.persistence.enabled (eq $role "consumer")) $root.Values.extraVolumeMounts }}
  volumeMounts:
    {{- if and $root.Values.retention.archiveBeforeDelete $root.Values.persistence.enabled (eq $role "consumer") }}
    - name: archive
      mountPath: {{ $root.Values.retention.archiveDir }}
    {{- end }}
    {{- with $root.Values.extraVolumeMounts }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  {{- end }}
  resources:
    {{- toYaml (index $root.Values $role).resources | nindent 4 }}
  livenessProbe:
    httpGet:
      path: /health
      port: http
    initialDelaySeconds: 30
    periodSeconds: 15
    timeoutSeconds: 5
    failureThreshold: 3
  readinessProbe:
    httpGet:
      path: /readyz
      port: http
    initialDelaySeconds: 10
    periodSeconds: 10
    timeoutSeconds: 5
    failureThreshold: 3
  startupProbe:
    httpGet:
      path: /health
      port: http
    initialDelaySeconds: 5
    periodSeconds: 5
    timeoutSeconds: 5
    failureThreshold: 30
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "db-monitor.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "db-monitor.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
