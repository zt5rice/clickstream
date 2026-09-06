{{- define "clickstream.labels" -}}
app.kubernetes.io/name: clickstream
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion }}
{{- end -}}
