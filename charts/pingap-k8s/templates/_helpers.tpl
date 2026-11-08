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

{{/* ---------------- Gateway server groups ---------------- */}}

{{/*
gateway.groups, checked and filled in, as a JSON list ([] when gateway is off). Each
item: name, service (the group's Service name), addresses (default: the Service's
DNS name), ports ([{port, targetPort, tls, server}], server = "gw-<name>-<port>").
*/}}
{{- define "pingap-k8s.gatewayGroups" -}}
{{- $out := list }}
{{- if .Values.gateway.enabled }}
{{- $names := list }}
{{- $targets := list }}
{{- range $i, $g := .Values.gateway.groups | default (list) }}
{{- $name := $g.name | default "" | toString }}
{{- if not (regexMatch "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$" $name) }}
{{- fail (printf "gateway.groups[%d].name: %q is not a DNS label" $i $name) }}
{{- end }}
{{- if has $name $names }}
{{- fail (printf "gateway.groups[%d].name: %q is used more than once" $i $name) }}
{{- end }}
{{- $names = append $names $name }}
{{- $service := printf "%s-gw-%s" (include "pingap-k8s.pingap.fullname" $) $name }}
{{- if gt (len $service) 63 }}
{{- fail (printf "gateway.groups[%d].name: the Service name %q is longer than 63 characters" $i $service) }}
{{- end }}
{{- $addresses := $g.addresses | default (list) }}
{{- if not $addresses }}
{{- if not $.Values.pingap.deploy }}
{{- fail (printf "gateway.groups[%d].addresses: required when pingap.deploy is false (the chart creates no Service for the group)" $i) }}
{{- end }}
{{- $addresses = list (printf "%s.%s.svc" $service $.Release.Namespace) }}
{{- end }}
{{- $ports := list }}
{{- $listenerPorts := list }}
{{- range $j, $p := $g.ports | default (list) }}
{{- $port := int ($p.port | default 0) }}
{{- $target := int ($p.targetPort | default 0) }}
{{- if or (lt $port 1) (gt $port 65535) (lt $target 1) (gt $target 65535) }}
{{- fail (printf "gateway.groups[%d].ports[%d]: port and targetPort must be 1-65535" $i $j) }}
{{- end }}
{{- if has $port $listenerPorts }}
{{- fail (printf "gateway.groups[%d].ports[%d]: port %d is listed more than once" $i $j $port) }}
{{- end }}
{{- if or (has $target $targets) (eq $target (int $.Values.pingap.httpPort)) (eq $target (int $.Values.pingap.admin.port)) }}
{{- fail (printf "gateway.groups[%d].ports[%d]: targetPort %d is already in use (another group, pingap.httpPort or the admin port)" $i $j $target) }}
{{- end }}
{{- $listenerPorts = append $listenerPorts $port }}
{{- $targets = append $targets $target }}
{{- $ports = append $ports (dict "port" $port "targetPort" $target "tls" ($p.tls | default false) "server" (printf "gw-%s-%d" $name $port) "settings" ($p.settings | default dict)) }}
{{- end }}
{{- if not $ports }}
{{- fail (printf "gateway.groups[%d].ports: at least one port is required" $i) }}
{{- end }}
{{- $out = append $out (dict "name" $name "service" $service "addresses" $addresses "ports" $ports) }}
{{- end }}
{{- end }}
{{- $out | toJson }}
{{- end }}

{{/* The seed config of the group servers (TOML), empty without groups. */}}
{{- define "pingap-k8s.gatewayGroupServers" -}}
{{- range (include "pingap-k8s.gatewayGroups" . | fromJsonArray) }}
{{- $group := .name }}
{{- range .ports }}

# Gateway server group {{ $group }}: listener port {{ int .port }}.
{{- $server := .server }}
[servers.{{ $server }}]
addr = "[::]:{{ int .targetPort }}"
{{- if .tls }}
global_certificates = true
{{- if not (hasKey .settings "strict_sni") }}
# 421 Misdirected Request when the Host belongs to another listener than the SNI.
strict_sni = true
{{- end }}
{{- end }}
{{- with .settings }}
{{- if or (hasKey . "addr") (hasKey . "global_certificates") (hasKey . "locations") }}
{{- fail (printf "gateway.groups: settings of server %s must not set addr, global_certificates or locations" $server) }}
{{- end }}
{{- include "pingap-k8s.tomlTable" . }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{/* ---------------- RBAC namespaces ---------------- */}}

{{/*
Namespaces of the Gateway API provider's backend Services and listener Secrets, as
a JSON list ([] when gateway is off).
*/}}
{{- define "pingap-k8s.gatewayNamespaces" -}}
{{- $nss := list }}
{{- if .Values.gateway.enabled }}
{{- range .Values.gateway.namespaces }}
{{- if not (regexMatch "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$" .) }}
{{- fail (printf "gateway.namespaces: %q is not a valid namespace name" .) }}
{{- end }}
{{- $nss = append $nss . }}
{{- end }}
{{- end }}
{{- $nss | uniq | sortAlpha | toJson }}
{{- end }}

{{/*
Namespaces the controller reads, as a JSON list: rbac.namespaces, the
gateway.namespaces when gateway is enabled, plus, with rbac.fromConfig, the
namespace of every config.upstreams[*].services entry
("<service>[.<namespace>]:<port> [weight]").
*/}}
{{- define "pingap-k8s.rbacNamespaces" -}}
{{- $default := default .Release.Namespace .Values.config.kubernetes.default_namespace }}
{{- $nss := list }}
{{- range .Values.rbac.namespaces }}
{{- $nss = append $nss . }}
{{- end }}
{{- range (include "pingap-k8s.gatewayNamespaces" . | fromJsonArray) }}
{{- $nss = append $nss . }}
{{- end }}
{{- if .Values.rbac.fromConfig }}
{{- range $name, $up := .Values.config.upstreams }}
{{- if $up }}
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
{{- end }}
{{- if not $nss }}
{{- $nss = list $default }}
{{- end }}
{{- $nss | uniq | sortAlpha | toJson }}
{{- end }}

{{/*
Checksum of the settings the controller reads at start only (everything else in
its config is reloaded live), put on the pod template so a change restarts the pods.
*/}}
{{- define "pingap-k8s.restartChecksum" -}}
{{- $restart := dict "events" (.Values.events.enabled | default false) "server" (.Values.config.server | default dict) -}}
{{- if .Values.gateway.enabled -}}
{{- $_ := set $restart "gateway" (dict "controllerName" .Values.gateway.controllerName "maxTxnOps" (.Values.gateway.maxTxnOps | default 64 | int)) -}}
{{- end -}}
{{- $restart | toJson | sha256sum | trunc 16 -}}
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
cluster_domain = {{ .Values.config.kubernetes.cluster_domain | default "cluster.local" | include "pingap-k8s.tomlString" }}
events = {{ ternary "true" "false" (.Values.events.enabled | default false) }}

[defaults]
{{- include "pingap-k8s.tomlTable" .Values.config.defaults }}
{{- if and .Values.config.server (hasKey .Values.config.server "status_listen") }}
{{- fail "config.server.status_listen: use status.enabled and status.port instead" }}
{{- end }}
{{- if or .Values.config.server .Values.status.enabled }}

[server]
{{- with .Values.config.server }}
{{- include "pingap-k8s.tomlTable" . }}
{{- end }}
{{- if .Values.status.enabled }}
status_listen = {{ printf "127.0.0.1:%d" (int .Values.status.port) | include "pingap-k8s.tomlString" }}
{{- end }}
{{- end }}
{{- if .Values.gateway.enabled }}

[gateway]
enabled = true
controller_name = {{ include "pingap-k8s.tomlString" (required "gateway.controllerName is required" .Values.gateway.controllerName) }}
servers = {{ include "pingap-k8s.tomlValue" (.Values.gateway.servers | default (list)) }}
external_name = {{ ternary "true" "false" (.Values.gateway.externalName | default false) }}
addresses = {{ include "pingap-k8s.tomlValue" (.Values.gateway.addresses | default (list)) }}
{{- with .Values.gateway.healthCheck }}
health_check = {{ include "pingap-k8s.tomlString" . }}
{{- end }}
max_txn_ops = {{ .Values.gateway.maxTxnOps | default 64 | int }}
{{- range (include "pingap-k8s.gatewayGroups" . | fromJsonArray) }}

[[gateway.groups]]
name = {{ include "pingap-k8s.tomlString" .name }}
{{- $servers := list }}
{{- $ports := list }}
{{- range .ports }}
{{- $servers = append $servers .server }}
{{- $ports = append $ports (printf "%d:%d" (int .port) (int .targetPort)) }}
{{- end }}
servers = {{ include "pingap-k8s.tomlValue" $servers }}
addresses = {{ include "pingap-k8s.tomlValue" .addresses }}
ports = {{ include "pingap-k8s.tomlValue" $ports }}
{{- end }}
{{- end }}
{{- range $name := keys .Values.config.upstreams | sortAlpha }}
{{- if not (regexMatch "^[A-Za-z0-9_-]+$" $name) }}
{{- fail (printf "config.upstreams: %q is not a valid Pingap upstream name ([A-Za-z0-9_-]+)" $name) }}
{{- end }}
{{- $up := index $.Values.config.upstreams $name }}
{{- if not $up }}
{{- continue }}
{{- end }}
{{- if not $up.services }}
{{- fail (printf "config.upstreams.%s: services is required" $name) }}
{{- end }}

[upstreams.{{ $name }}]
{{- include "pingap-k8s.tomlTable" $up }}
{{- end }}
{{- end }}
