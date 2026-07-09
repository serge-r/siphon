package collector

type SchemaField struct {
	Name string
	Type string
}

type JSONSchema struct {
	Schema               string                    `json:"$schema"`
	Type                 string                    `json:"type"`
	AdditionalProperties bool                      `json:"additionalProperties"`
	Properties           map[string]JSONSchemaProp `json:"properties"`
	Required             []string                  `json:"required"`
}

type JSONSchemaProp struct {
	Type                 string                    `json:"type,omitempty"`
	Format               string                    `json:"format,omitempty"`
	AdditionalProperties bool                      `json:"additionalProperties,omitempty"`
	Properties           map[string]JSONSchemaProp `json:"properties,omitempty"`
}

func BuildJSONSchema(enrichers []Enricher) JSONSchema {
	var fields []SchemaField
	for _, enricher := range enrichers {
		fields = append(fields, enricher.Fields()...)
	}
	return buildJSONSchema(fields)
}

func BuildJSONSchemaForConfig(cfg Config) JSONSchema {
	var fields []SchemaField
	if len(cfg.Enrichment.Static) > 0 {
		fields = append(fields,
			SchemaField{Name: "source_static_name", Type: "string"},
			SchemaField{Name: "destination_static_name", Type: "string"},
		)
	}
	if cfg.Enrichment.Inventory.Enabled {
		fields = append(fields,
			SchemaField{Name: "source_inventory_name", Type: "string"},
			SchemaField{Name: "destination_inventory_name", Type: "string"},
		)
	}
	if cfg.Enrichment.Kubernetes.Enabled {
		fields = append(fields,
			SchemaField{Name: "source_namespace", Type: "string"},
			SchemaField{Name: "source_pod_name", Type: "string"},
			SchemaField{Name: "destination_namespace", Type: "string"},
			SchemaField{Name: "destination_pod_name", Type: "string"},
		)
	}
	if cfg.Enrichment.AWSIPRanges.Enabled {
		fields = append(fields, awsIPRangesSchemaFields()...)
	}
	if cfg.Enrichment.DNS.Enabled {
		fields = append(fields,
			SchemaField{Name: "source_dns_name", Type: "string"},
			SchemaField{Name: "destination_dns_name", Type: "string"},
		)
	}
	if cfg.Enrichment.GeoIP.Enabled {
		fields = append(fields, geoIPSchemaFields()...)
	}
	return buildJSONSchema(fields)
}

func buildJSONSchema(fields []SchemaField) JSONSchema {
	properties := map[string]JSONSchemaProp{
		"sequence":           {Type: "integer"},
		"observation_domain": {Type: "integer"},
		"export_time":        {Type: "string", Format: "date-time"},
		"received_at":        {Type: "string", Format: "date-time"},
		"exporter":           {Type: "string"},
		"source_ip":          {Type: "string"},
		"source_name":        {Type: "string"},
		"source_port":        {Type: "integer"},
		"destination_ip":     {Type: "string"},
		"destination_name":   {Type: "string"},
		"destination_port":   {Type: "integer"},
		"protocol":           {Type: "integer"},
		"packets":            {Type: "integer"},
		"bytes":              {Type: "integer"},
	}
	for _, field := range fields {
		properties[field.Name] = JSONSchemaProp{Type: field.Type}
	}

	return JSONSchema{
		Schema:               "https://json-schema.org/draft/2020-12/schema",
		Type:                 "object",
		AdditionalProperties: false,
		Required: []string{
			"sequence",
			"observation_domain",
			"export_time",
			"received_at",
			"exporter",
			"source_ip",
			"source_port",
			"destination_ip",
			"destination_port",
			"protocol",
			"packets",
			"bytes",
		},
		Properties: properties,
	}
}

func awsIPRangesSchemaFields() []SchemaField {
	return []SchemaField{
		{Name: "source_aws_service", Type: "string"},
		{Name: "source_aws_region", Type: "string"},
		{Name: "destination_aws_service", Type: "string"},
		{Name: "destination_aws_region", Type: "string"},
	}
}

func geoIPSchemaFields() []SchemaField {
	return []SchemaField{
		{Name: "source_geoip_isp", Type: "string"},
		{Name: "source_geoip_asn", Type: "integer"},
		{Name: "source_geoip_country", Type: "string"},
		{Name: "source_geoip_city", Type: "string"},
		{Name: "destination_geoip_isp", Type: "string"},
		{Name: "destination_geoip_asn", Type: "integer"},
		{Name: "destination_geoip_country", Type: "string"},
		{Name: "destination_geoip_city", Type: "string"},
	}
}
