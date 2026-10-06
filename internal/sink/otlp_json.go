package sink

import (
	"encoding/hex"
	"encoding/json"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// OTLP JSON uses numeric enums and hex IDs instead of protobuf JSON's base64 IDs.
func marshalOTLPJSON(msg proto.Message) ([]byte, error) {
	body, err := (protojson.MarshalOptions{UseEnumNumbers: true}).Marshal(msg)
	if err != nil {
		return nil, err
	}
	return hexJSONIDs(body, msg.ProtoReflect())
}

// Walk message fields, not arbitrary JSON keys: attributes named traceId must
// retain their original values. RawMessage preserves 64-bit integer precision.
func hexJSONIDs(body []byte, msg protoreflect.Message) ([]byte, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return nil, err
	}
	var walkErr error
	msg.Range(func(fd protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		key := fd.JSONName()
		if fd.Kind() == protoreflect.BytesKind {
			switch fd.Name() {
			case "trace_id", "span_id", "parent_span_id":
				fields[key], walkErr = json.Marshal(hex.EncodeToString(value.Bytes()))
			}
		} else if fd.Kind() == protoreflect.MessageKind && !fd.IsMap() {
			if fd.IsList() {
				var items []json.RawMessage
				walkErr = json.Unmarshal(fields[key], &items)
				if walkErr == nil {
					for i := range items {
						items[i], walkErr = hexJSONIDs(items[i], value.List().Get(i).Message())
						if walkErr != nil {
							break
						}
					}
					if walkErr == nil {
						fields[key], walkErr = json.Marshal(items)
					}
				}
			} else {
				fields[key], walkErr = hexJSONIDs(fields[key], value.Message())
			}
		}
		return walkErr == nil
	})
	if walkErr != nil {
		return nil, walkErr
	}
	return json.Marshal(fields)
}
