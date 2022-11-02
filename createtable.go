package dynamo

import (
	"context"
	"encoding"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// StreamView determines what information is written to a table's stream.
type StreamView string

var (
	// Only the key attributes of the modified item are written to the stream.
	KeysOnlyView = StreamView(types.StreamViewTypeKeysOnly)
	// The entire item, as it appears after it was modified, is written to the stream.
	NewImageView = StreamView(types.StreamViewTypeNewImage)
	// The entire item, as it appeared before it was modified, is written to the stream.
	OldImageView = StreamView(types.StreamViewTypeOldImage)
	// Both the new and the old item images of the item are written to the stream.
	NewAndOldImagesView = StreamView(types.StreamViewTypeNewAndOldImages)
)

// IndexProjection determines which attributes are mirrored into indices.
type IndexProjection string

var (
	// Only the key attributes of the modified item are written to the stream.
	KeysOnlyProjection = IndexProjection(types.ProjectionTypeKeysOnly)
	// All of the table attributes are projected into the index.
	AllProjection = IndexProjection(types.ProjectionTypeAll)
	// Only the specified table attributes are projected into the index.
	IncludeProjection = IndexProjection(types.ProjectionTypeInclude)
)

// CreateTable is a request to create a new table.
// See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html
type CreateTable struct {
	db                      *DB
	tableName               string
	attribs                 []types.AttributeDefinition
	schema                  []types.KeySchemaElement
	globalIndices           map[string]types.GlobalSecondaryIndex
	localIndices            map[string]types.LocalSecondaryIndex
	readUnits               int64
	writeUnits              int64
	streamView              StreamView
	ondemand                bool
	tags                    []types.Tag
	encryptionSpecification *types.SSESpecification
	err                     error
}

// CreateTable begins a new operation to create a table with the given name.
// The second parameter must be a struct with appropriate hash and range key struct tags
// for the primary key and all indices.
//
// An example of a from struct follows:
// 	type UserAction struct {
// 		UserID string    `dynamo:"ID,hash" index:"Seq-ID-index,range"`
// 		Time   time.Time `dynamo:",range"`
// 		Seq    int64     `localIndex:"ID-Seq-index,range" index:"Seq-ID-index,hash"`
// 		UUID   string    `index:"UUID-index,hash"`
// 	}
// This creates a table with the primary hash key ID and range key Time.
// It creates two global secondary indices called UUID-index and Seq-ID-index,
// and a local secondary index called ID-Seq-index.
func (db *DB) CreateTable(name string, from interface{}) *CreateTable {
	ct := &CreateTable{
		db:            db,
		tableName:     name,
		schema:        []types.KeySchemaElement{},
		globalIndices: make(map[string]types.GlobalSecondaryIndex),
		localIndices:  make(map[string]types.LocalSecondaryIndex),
		readUnits:     1,
		writeUnits:    1,
		tags:          []types.Tag{},
	}
	rv := reflect.ValueOf(from)
	ct.setError(ct.from(rv))
	return ct
}

// OnDemand specifies to create the table with on-demand (pay per request) billing mode,
// if enabled. On-demand mode is disabled by default.
func (ct *CreateTable) OnDemand(enabled bool) *CreateTable {
	ct.ondemand = enabled
	return ct
}

// Provision specifies the provisioned read and write capacity for this table.
// If Provision isn't called and on-demand mode is disabled, the table will be created with 1 unit each.
func (ct *CreateTable) Provision(readUnits, writeUnits int64) *CreateTable {
	ct.readUnits, ct.writeUnits = readUnits, writeUnits
	return ct
}

// ProvisionIndex specifies the provisioned read and write capacity for the given
// global secondary index. Local secondary indices share their capacity with the table.
func (ct *CreateTable) ProvisionIndex(index string, readUnits, writeUnits int64) *CreateTable {
	idx := ct.globalIndices[index]
	idx.ProvisionedThroughput = &types.ProvisionedThroughput{
		ReadCapacityUnits:  &readUnits,
		WriteCapacityUnits: &writeUnits,
	}
	ct.globalIndices[index] = idx
	return ct
}

// Stream enables DynamoDB Streams for this table which the specified type of view.
// Streams are disabled by default.
func (ct *CreateTable) Stream(view StreamView) *CreateTable {
	ct.streamView = view
	return ct
}

// Project specifies the projection type for the given table.
// When using IncludeProjection, you must specify the additional attributes to include via includeAttribs.
func (ct *CreateTable) Project(index string, projection IndexProjection, includeAttribs ...string) *CreateTable {
	projectionStr := types.ProjectionType(projection)
	proj := &types.Projection{
		ProjectionType: projectionStr,
	}
	if projection == IncludeProjection {
	attribs:
		for _, attr := range includeAttribs {
			for _, a := range proj.NonKeyAttributes {
				if attr == a {
					continue attribs
				}
			}
			proj.NonKeyAttributes = append(proj.NonKeyAttributes, attr)
		}
	}
	if idx, global := ct.globalIndices[index]; global {
		idx.Projection = proj
		ct.globalIndices[index] = idx
	} else if localIdx, ok := ct.localIndices[index]; ok {
		localIdx.Projection = proj
		ct.localIndices[index] = localIdx
	} else {
		ct.setError(fmt.Errorf("dynamo: no such index: %s", index))
	}
	return ct
}

// Index specifies an index to add to this table.
func (ct *CreateTable) Index(index Index) *CreateTable {
	ct.add(index.HashKey, string(index.HashKeyType))
	ks := []types.KeySchemaElement{
		{
			AttributeName: &index.HashKey,
			KeyType:       types.KeyTypeHash,
		},
	}
	if index.RangeKey != "" {
		ct.add(index.RangeKey, string(index.RangeKeyType))
		ks = append(ks, types.KeySchemaElement{
			AttributeName: &index.RangeKey,
			KeyType:       types.KeyTypeRange,
		})
	}

	var proj *types.Projection
	if index.ProjectionType != "" {
		proj = &types.Projection{
			ProjectionType: types.ProjectionType(index.ProjectionType),
		}
		if index.ProjectionType == IncludeProjection {
			proj.NonKeyAttributes = index.ProjectionAttribs
		}
	}

	if index.Local {
		idx := ct.localIndices[index.Name]
		idx.KeySchema = ks
		if proj != nil {
			idx.Projection = proj
		}
		ct.localIndices[index.Name] = idx
		return ct
	}

	idx := ct.globalIndices[index.Name]
	idx.KeySchema = ks
	if index.Throughput.Read != 0 || index.Throughput.Write != 0 {
		idx.ProvisionedThroughput = &types.ProvisionedThroughput{
			ReadCapacityUnits:  &index.Throughput.Read,
			WriteCapacityUnits: &index.Throughput.Write,
		}
	}
	if proj != nil {
		idx.Projection = proj
	}
	ct.globalIndices[index.Name] = idx
	return ct
}

// Tag specifies a metadata tag for this table. Multiple tags may be specified.
func (ct *CreateTable) Tag(key, value string) *CreateTable {
	for _, tag := range ct.tags {
		if *tag.Key == key {
			*tag.Value = value
			return ct
		}
	}
	tag := types.Tag{
		Key:   aws.String(key),
		Value: aws.String(value),
	}
	ct.tags = append(ct.tags, tag)
	return ct
}

// SSEEncryption specifies the server side encryption for this table.
// Encryption is disabled by default.
func (ct *CreateTable) SSEEncryption(enabled bool, keyID string, sseType SSEType) *CreateTable {
	encryption := types.SSESpecification{
		Enabled:        aws.Bool(enabled),
		KMSMasterKeyId: aws.String(keyID),
		SSEType:        types.SSEType(string(sseType)),
	}
	ct.encryptionSpecification = &encryption
	return ct
}

// Run creates this table or returns an error.
func (ct *CreateTable) Run() error {
	ctx, cancel := defaultContext()
	defer cancel()
	return ct.RunWithContext(ctx)
}

// RunWithContext creates this table or returns an error.
func (ct *CreateTable) RunWithContext(ctx context.Context) error {
	if ct.err != nil {
		return ct.err
	}

	input := ct.input()
	return retry(ctx, func() error {
		_, err := ct.db.client.CreateTable(ctx, input)
		return err
	})
}

// Wait creates this table and blocks until it exists and is ready to use.
func (ct *CreateTable) Wait() error {
	ctx, cancel := defaultContext()
	defer cancel()
	return ct.WaitWithContext(ctx)
}

// WaitWithContext creates this table and blocks until it exists and is ready to use.
func (ct *CreateTable) WaitWithContext(ctx context.Context) error {
	if err := ct.RunWithContext(ctx); err != nil {
		return err
	}
	return ct.db.Table(ct.tableName).WaitWithContext(ctx)
}

func (ct *CreateTable) from(rv reflect.Value) error {
	switch rv.Kind() {
	case reflect.Struct: // ok
	case reflect.Ptr:
		return ct.from(rv.Elem())
	default:
		return fmt.Errorf("dynamo: CreateTable example must be a struct")
	}

	for i := 0; i < rv.Type().NumField(); i++ {
		field := rv.Type().Field(i)
		fv := rv.Field(i)

		name, _ := fieldInfo(field)
		if name == "-" {
			// skip
			continue
		}

		// inspect anonymous structs
		if fv.Type().Kind() == reflect.Struct && field.Anonymous {
			if err := ct.from(fv); err != nil {
				return err
			}
		}

		// primary keys
		if keyType := keyTypeFromTag(field.Tag.Get("dynamo")); keyType != "" {
			ct.add(name, typeOf(fv, field.Tag.Get("dynamo")))
			ct.schema = append(ct.schema, types.KeySchemaElement{
				AttributeName: &name,
				KeyType:       types.KeyType(keyType),
			})
		}

		// global secondary index
		if gsi, ok := tagLookup(string(field.Tag), "index"); ok {
			for _, index := range gsi {
				ct.add(name, typeOf(fv, field.Tag.Get("dynamo")))
				keyType := keyTypeFromTag(index)
				indexName := index[:len(index)-len(keyType)-1]
				idx := ct.globalIndices[indexName]
				idx.KeySchema = append(idx.KeySchema, types.KeySchemaElement{
					AttributeName: &name,
					KeyType:       types.KeyType(keyType),
				})
				ct.globalIndices[indexName] = idx
			}
		}

		// local secondary index
		if lsi, ok := tagLookup(string(field.Tag), "localIndex"); ok {
			for _, localIndex := range lsi {
				ct.add(name, typeOf(fv, field.Tag.Get("dynamo")))
				keyType := keyTypeFromTag(localIndex)
				indexName := localIndex[:len(localIndex)-len(keyType)-1]
				idx := ct.localIndices[indexName]
				idx.KeySchema = append(idx.KeySchema, types.KeySchemaElement{
					AttributeName: &name,
					KeyType:       types.KeyType(keyType),
				})
				ct.localIndices[indexName] = idx
			}
		}
	}

	return nil
}

func (ct *CreateTable) input() *dynamodb.CreateTableInput {
	sortKeySchemas(ct.schema)
	input := &dynamodb.CreateTableInput{
		TableName:            &ct.tableName,
		AttributeDefinitions: ct.attribs,
		KeySchema:            ct.schema,
		SSESpecification:     ct.encryptionSpecification,
	}
	if ct.ondemand {
		input.BillingMode = types.BillingModePayPerRequest
	} else {
		input.ProvisionedThroughput = &types.ProvisionedThroughput{
			ReadCapacityUnits:  &ct.readUnits,
			WriteCapacityUnits: &ct.writeUnits,
		}
	}
	if ct.streamView != "" {
		enabled := true
		view := string(ct.streamView)
		input.StreamSpecification = &types.StreamSpecification{
			StreamEnabled:  &enabled,
			StreamViewType: types.StreamViewType(view),
		}
	}
	for name, idx := range ct.localIndices {
		name, idx := name, idx
		idx.IndexName = &name
		if idx.Projection == nil {
			all := string(AllProjection)
			idx.Projection = &types.Projection{
				ProjectionType: types.ProjectionType(all),
			}
		}
		// add the primary hash key
		if len(idx.KeySchema) == 1 {
			idx.KeySchema = []types.KeySchemaElement{
				ct.schema[0],
				idx.KeySchema[0],
			}
		}
		sortKeySchemas(idx.KeySchema)
		input.LocalSecondaryIndexes = append(input.LocalSecondaryIndexes, idx)
	}
	for name, idx := range ct.globalIndices {
		name, idx := name, idx
		idx.IndexName = &name
		if idx.Projection == nil {
			all := string(AllProjection)
			idx.Projection = &types.Projection{
				ProjectionType: types.ProjectionType(all),
			}
		}
		if ct.ondemand {
			idx.ProvisionedThroughput = nil
		} else if idx.ProvisionedThroughput == nil {
			units := int64(1)
			idx.ProvisionedThroughput = &types.ProvisionedThroughput{
				ReadCapacityUnits:  &units,
				WriteCapacityUnits: &units,
			}
		}
		sortKeySchemas(idx.KeySchema)
		input.GlobalSecondaryIndexes = append(input.GlobalSecondaryIndexes, idx)
	}
	if len(ct.tags) > 0 {
		input.Tags = ct.tags
	}
	return input
}

func (ct *CreateTable) add(name string, typ string) {
	if typ == "" {
		ct.setError(fmt.Errorf("dynamo: invalid type for key: %s", name))
		return
	}

	for _, attr := range ct.attribs {
		if *attr.AttributeName == name {
			return
		}
	}

	ct.attribs = append(ct.attribs, types.AttributeDefinition{
		AttributeName: &name,
		AttributeType: types.ScalarAttributeType(typ),
	})
}

func (ct *CreateTable) setError(err error) {
	if ct.err == nil {
		ct.err = err
	}
}

func typeOf(rv reflect.Value, tag string) string {
	split := strings.Split(tag, ",")
	if len(split) > 1 {
		for _, v := range split[1:] {
			if v == "unixtime" {
				return "N"
			}
		}
	}
	if rv.CanInterface() {
		switch x := rv.Interface().(type) {
		case Marshaler:
			if av, err := x.MarshalDynamo(); err == nil {
				if iface, err := av2iface(av); err == nil {
					return typeOf(reflect.ValueOf(iface), tag)
				}
			}
		case attributevalue.Marshaler:

			if av, err := x.MarshalDynamoDBAttributeValue(); err == nil {
				if iface, err := av2iface(av); err == nil {
					return typeOf(reflect.ValueOf(iface), tag)
				}
			}
		case encoding.TextMarshaler:
			return "S"
		}
	}

	typ := rv.Type()
check:
	switch typ.Kind() {
	case reflect.Ptr:
		typ = typ.Elem()
		goto check
	case reflect.String:
		return "S"
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16,
		reflect.Int8, reflect.Float64, reflect.Float32,
		reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		return "N"
	case reflect.Slice, reflect.Array:
		if typ.Elem().Kind() == reflect.Uint8 {
			return "B"
		}
	}

	return ""
}

func keyTypeFromTag(tag string) types.KeyType {
	split := strings.Split(tag, ",")
	if len(split) <= 1 {
		return ""
	}
	for _, v := range split[1:] {
		switch v {
		case "hash", "partition":
			return types.KeyTypeHash
		case "range", "sort":
			return types.KeyTypeRange
		}
	}
	return ""
}

func sortKeySchemas(schemas []types.KeySchemaElement) {
	if schemas[0].KeyType == types.KeyTypeRange {
		schemas[0], schemas[1] = schemas[1], schemas[0]
	}
}

// ripped from the stdlib
// Copyright 2009 The Go Authors. All rights reserved.
func tagLookup(tag, key string) (value []string, ok bool) {
	for tag != "" {
		// Skip leading space.
		i := 0
		for i < len(tag) && tag[i] == ' ' {
			i++
		}
		tag = tag[i:]
		if tag == "" {
			break
		}

		// Scan to colon. A space, a quote or a control character is a syntax error.
		// Strictly speaking, control chars include the range [0x7f, 0x9f], not just
		// [0x00, 0x1f], but in practice, we ignore the multi-byte control characters
		// as it is simpler to inspect the tag's bytes than the tag's runes.
		i = 0
		for i < len(tag) && tag[i] > ' ' && tag[i] != ':' && tag[i] != '"' && tag[i] != 0x7f {
			i++
		}
		if i == 0 || i+1 >= len(tag) || tag[i] != ':' || tag[i+1] != '"' {
			break
		}
		name := string(tag[:i])
		tag = tag[i+1:]

		// Scan quoted string to find value.
		i = 1
		for i < len(tag) && tag[i] != '"' {
			if tag[i] == '\\' {
				i++
			}
			i++
		}
		if i >= len(tag) {
			break
		}
		qvalue := string(tag[:i+1])
		tag = tag[i+1:]

		if key == name {
			v, err := strconv.Unquote(qvalue)
			if err != nil {
				break
			}
			value = append(value, v)
		}
	}
	return value, len(value) > 0
}
