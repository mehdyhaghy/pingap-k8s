{{/* Chart name. */}}
{{- define "pingap-k8s.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Full name of the release, used for the controller's objects. */}}
{{- define "pingap-k8s.fullname" -}}
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

{{- define "pingap-k8s.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Labels shared by every object of the release. */}}
{{- define "pingap-k8s.commonLabels" -}}
helm.sh/chart: {{ include "pingap-k8s.chart" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: pingap-k8s
{{- end }}

{{/* Controller labels. */}}
{{- define "pingap-k8s.labels" -}}
{{ include "pingap-k8s.commonLabels" . }}
{{ include "pingap-k8s.selectorLabels" . }}
{{- end }}

{{- define "pingap-k8s.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pingap-k8s.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: controller
{{- end }}

{{- define "pingap-k8s.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "pingap-k8s.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "pingap-k8s.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end }}

{{/* ---------------- etcd ---------------- */}}

{{- define "pingap-k8s.etcd.fullname" -}}
{{- printf "%s-etcd" (include "pingap-k8s.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "pingap-k8s.etcd.headless" -}}
{{- printf "%s-etcd-headless" (include "pingap-k8s.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "pingap-k8s.etcd.labels" -}}
{{ include "pingap-k8s.commonLabels" . }}
{{ include "pingap-k8s.etcd.selectorLabels" . }}
{{- end }}

{{- define "pingap-k8s.etcd.selectorLabels" -}}
app.kubernetes.io/name: etcd
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: etcd
{{- end }}

{{/* Directory where etcd client TLS files are mounted, in the controller and in Pingap. */}}
{{- define "pingap-k8s.etcd.tlsDir" -}}
/etc/pingap-k8s/etcd
{{- end }}

{{/* The etcd URL of the controller. */}}
{{- define "pingap-k8s.etcd.url" -}}
{{- if .Values.etcd.url }}
{{- .Values.etcd.url }}
{{- else if .Values.etcd.deploy }}
{{- $dir := include "pingap-k8s.etcd.tlsDir" . }}
{{- $k := .Values.etcd.tlsKeys }}
{{- printf "etcd://%s.%s.svc:2379/%s?ca=%s/%s&cert=%s/%s&key=%s/%s" (include "pingap-k8s.etcd.fullname" .) .Release.Namespace .Values.etcd.prefix $dir $k.ca $dir $k.cert $dir $k.key }}
{{- else }}
{{- "etcd://etcd:2379/pingap" }}
{{- end }}
{{- end }}

{{/* Secret with the controller's etcd client TLS files ("" when none). */}}
{{- define "pingap-k8s.etcd.clientSecret" -}}
{{- if .Values.etcd.tlsSecret }}
{{- .Values.etcd.tlsSecret }}
{{- else if and .Values.etcd.deploy .Values.etcd.tls.autoGenerate }}
{{- printf "%s-client" (include "pingap-k8s.etcd.fullname" .) }}
{{- else if .Values.etcd.deploy }}
{{- fail "etcd.deploy with etcd.tls.autoGenerate=false needs etcd.tlsSecret (the controller's etcd client certificate)" }}
{{- end }}
{{- end }}

{{/* Secret with the etcd server (and peer) certificate of the bundled etcd. */}}
{{- define "pingap-k8s.etcd.serverSecret" -}}
{{- if .Values.etcd.tls.autoGenerate }}
{{- printf "%s-server" (include "pingap-k8s.etcd.fullname" .) }}
{{- else }}
{{- required "etcd.deploy with etcd.tls.autoGenerate=false needs etcd.tls.serverSecret" .Values.etcd.tls.serverSecret }}
{{- end }}
{{- end }}

{{/* Secret with the quickstart Pingap's etcd client TLS files ("" when none). */}}
{{- define "pingap-k8s.pingap.etcdSecret" -}}
{{- if .Values.pingap.etcdTlsSecret }}
{{- .Values.pingap.etcdTlsSecret }}
{{- else if and .Values.etcd.deploy .Values.etcd.tls.autoGenerate }}
{{- printf "%s-pingap-client" (include "pingap-k8s.etcd.fullname" .) }}
{{- else if and .Values.etcd.deploy .Values.etcd.tls.pingapClientSecret }}
{{- .Values.etcd.tls.pingapClientSecret }}
{{- else }}
{{- .Values.etcd.tlsSecret }}
{{- end }}
{{- end }}

{{/* ---------------- quickstart Pingap ---------------- */}}

{{- define "pingap-k8s.pingap.fullname" -}}
{{- printf "%s-pingap" (include "pingap-k8s.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "pingap-k8s.pingap.labels" -}}
{{ include "pingap-k8s.commonLabels" . }}
{{ include "pingap-k8s.pingap.selectorLabels" . }}
{{- end }}

{{- define "pingap-k8s.pingap.selectorLabels" -}}
app.kubernetes.io/name: pingap
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: proxy
{{- end }}

{{- define "pingap-k8s.pingap.etcdUrl" -}}
{{- default (include "pingap-k8s.etcd.url" .) .Values.pingap.etcdUrl }}
{{- end }}

{{- define "pingap-k8s.pingap.adminSecret" -}}
{{- default (printf "%s-admin" (include "pingap-k8s.pingap.fullname" .)) .Values.pingap.admin.existingSecret }}
{{- end }}

{{/* ---------------- RBAC namespaces ---------------- */}}

{{/*
Namespaces the controller reads, as a JSON list: rbac.namespaces plus, with
rbac.fromConfig, the namespace of every config.upstreams[*].services entry
("<service>[.<namespace>]:<port> [weight]").
*/}}
{{- define "pingap-k8s.rbacNamespaces" -}}
{{- $default := default .Release.Namespace .Values.config.kubernetes.default_namespace }}
{{- $nss := list }}
{{- range .Values.rbac.namespaces }}
{{- $nss = append $nss . }}
{{- end }}
{{- if .Values.rbac.fromConfig }}
{{- range $name, $up := .Values.config.upstreams }}
{{- range $entry := (default (list) $up.services) }}
{{- $target := first (regexSplit "\\s+" (trim $entry) -1) }}
{{- $svc := regexReplaceAll ":[^:]*$" $target "" }}
{{- if contains "." $svc }}
{{- $nss = append $nss (index (splitn "." 2 $svc) "_1") }}
{{- else }}
{{- $nss = append $nss $default }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
{{- if not $nss }}
{{- $nss = list $default }}
{{- end }}
{{- $nss | uniq | sortAlpha | toJson }}
{{- end }}

{{/* ---------------- TOML rendering ---------------- */}}

{{/* A TOML basic string. JSON escaping is valid TOML; the HTML escapes are undone for readability. */}}
{{- define "pingap-k8s.tomlString" -}}
{{- toJson . | replace "\\u0026" "&" | replace "\\u003c" "<" | replace "\\u003e" ">" }}
{{- end }}

{{/* One TOML value: string, bool, number or a list of those. */}}
{{- define "pingap-k8s.tomlValue" -}}
{{- if kindIs "string" . }}
{{- include "pingap-k8s.tomlString" . }}
{{- else if kindIs "bool" . }}
{{- ternary "true" "false" . }}
{{- else if or (kindIs "float64" .) (kindIs "float32" .) }}
{{- if eq (float64 (int64 .)) (float64 .) }}{{ int64 . }}{{ else }}{{ . }}{{ end }}
{{- else if or (kindIs "int" .) (kindIs "int64" .) }}
{{- . }}
{{- else if kindIs "slice" . }}
{{- $items := list }}
{{- range . }}
{{- $items = append $items (include "pingap-k8s.tomlValue" .) }}
{{- end }}
{{- printf "[%s]" (join ", " $items) }}
{{- else }}
{{- fail (printf "config: unsupported value %v (use strings, booleans, numbers or lists)" .) }}
{{- end }}
{{- end }}

{{/* "key = value" lines of a flat map, keys sorted, "services" first. */}}
{{- define "pingap-k8s.tomlTable" -}}
{{- if hasKey . "services" }}
services = {{ include "pingap-k8s.tomlValue" .services }}
{{- end }}
{{- range $k := keys . | sortAlpha }}
{{- if ne $k "services" }}
{{- if not (regexMatch "^[A-Za-z0-9_-]+$" $k) }}
{{- fail (printf "config: invalid key %q" $k) }}
{{- end }}
{{ $k }} = {{ include "pingap-k8s.tomlValue" (index $ $k) }}
{{- end }}
{{- end }}
{{- end }}

{{/* The controller config file. */}}
{{- define "pingap-k8s.configToml" -}}
# Rendered by the pingap-k8s Helm chart.
[etcd]
url = {{ include "pingap-k8s.tomlString" (include "pingap-k8s.etcd.url" .) }}

[kubernetes]
kubeconfig = {{ .Values.config.kubernetes.kubeconfig | default "" | include "pingap-k8s.tomlString" }}
context = {{ .Values.config.kubernetes.context | default "" | include "pingap-k8s.tomlString" }}
default_namespace = {{ .Values.config.kubernetes.default_namespace | default "" | include "pingap-k8s.tomlString" }}

[defaults]
{{- include "pingap-k8s.tomlTable" .Values.config.defaults }}
{{- if .Values.config.server }}

[server]
{{- include "pingap-k8s.tomlTable" .Values.config.server }}
{{- end }}
{{- range $name := keys .Values.config.upstreams | sortAlpha }}
{{- if not (regexMatch "^[A-Za-z0-9_-]+$" $name) }}
{{- fail (printf "config.upstreams: %q is not a valid Pingap upstream name ([A-Za-z0-9_-]+)" $name) }}
{{- end }}
{{- $up := index $.Values.config.upstreams $name }}
{{- if not $up.services }}
{{- fail (printf "config.upstreams.%s: services is required" $name) }}
{{- end }}

[upstreams.{{ $name }}]
{{- include "pingap-k8s.tomlTable" $up }}
{{- end }}
{{- end }}
