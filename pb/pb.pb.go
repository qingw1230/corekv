// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.35.1
// 	protoc        v3.12.4
// source: pb/pb.proto

package pb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type ManifestChange_Operation int32

const (
	ManifestChange_CREATE ManifestChange_Operation = 0
	ManifestChange_DELETE ManifestChange_Operation = 1
)

// Enum value maps for ManifestChange_Operation.
var (
	ManifestChange_Operation_name = map[int32]string{
		0: "CREATE",
		1: "DELETE",
	}
	ManifestChange_Operation_value = map[string]int32{
		"CREATE": 0,
		"DELETE": 1,
	}
)

func (x ManifestChange_Operation) Enum() *ManifestChange_Operation {
	p := new(ManifestChange_Operation)
	*p = x
	return p
}

func (x ManifestChange_Operation) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ManifestChange_Operation) Descriptor() protoreflect.EnumDescriptor {
	return file_pb_pb_proto_enumTypes[0].Descriptor()
}

func (ManifestChange_Operation) Type() protoreflect.EnumType {
	return &file_pb_pb_proto_enumTypes[0]
}

func (x ManifestChange_Operation) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use ManifestChange_Operation.Descriptor instead.
func (ManifestChange_Operation) EnumDescriptor() ([]byte, []int) {
	return file_pb_pb_proto_rawDescGZIP(), []int{5, 0}
}

type KV struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key       []byte `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value     []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	Version   uint64 `protobuf:"varint,3,opt,name=version,proto3" json:"version,omitempty"`
	ExpiresAt uint64 `protobuf:"varint,4,opt,name=expires_at,json=expiresAt,proto3" json:"expires_at,omitempty"`
	Meta      []byte `protobuf:"bytes,5,opt,name=meta,proto3" json:"meta,omitempty"`
}

func (x *KV) Reset() {
	*x = KV{}
	mi := &file_pb_pb_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *KV) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*KV) ProtoMessage() {}

func (x *KV) ProtoReflect() protoreflect.Message {
	mi := &file_pb_pb_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use KV.ProtoReflect.Descriptor instead.
func (*KV) Descriptor() ([]byte, []int) {
	return file_pb_pb_proto_rawDescGZIP(), []int{0}
}

func (x *KV) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *KV) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *KV) GetVersion() uint64 {
	if x != nil {
		return x.Version
	}
	return 0
}

func (x *KV) GetExpiresAt() uint64 {
	if x != nil {
		return x.ExpiresAt
	}
	return 0
}

func (x *KV) GetMeta() []byte {
	if x != nil {
		return x.Meta
	}
	return nil
}

type KVList struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Kvs []*KV `protobuf:"bytes,1,rep,name=kvs,proto3" json:"kvs,omitempty"`
}

func (x *KVList) Reset() {
	*x = KVList{}
	mi := &file_pb_pb_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *KVList) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*KVList) ProtoMessage() {}

func (x *KVList) ProtoReflect() protoreflect.Message {
	mi := &file_pb_pb_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use KVList.ProtoReflect.Descriptor instead.
func (*KVList) Descriptor() ([]byte, []int) {
	return file_pb_pb_proto_rawDescGZIP(), []int{1}
}

func (x *KVList) GetKvs() []*KV {
	if x != nil {
		return x.Kvs
	}
	return nil
}

// TableIndex sst 文件的索引
type TableIndex struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Offsets       []*BlockOffset `protobuf:"bytes,1,rep,name=offsets,proto3" json:"offsets,omitempty"`
	MaxVersion    uint64         `protobuf:"varint,2,opt,name=maxVersion,proto3" json:"maxVersion,omitempty"`
	KeyCount      uint32         `protobuf:"varint,3,opt,name=keyCount,proto3" json:"keyCount,omitempty"`
	StaleDataSize uint32         `protobuf:"varint,4,opt,name=staleDataSize,proto3" json:"staleDataSize,omitempty"`
}

func (x *TableIndex) Reset() {
	*x = TableIndex{}
	mi := &file_pb_pb_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TableIndex) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TableIndex) ProtoMessage() {}

func (x *TableIndex) ProtoReflect() protoreflect.Message {
	mi := &file_pb_pb_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TableIndex.ProtoReflect.Descriptor instead.
func (*TableIndex) Descriptor() ([]byte, []int) {
	return file_pb_pb_proto_rawDescGZIP(), []int{2}
}

func (x *TableIndex) GetOffsets() []*BlockOffset {
	if x != nil {
		return x.Offsets
	}
	return nil
}

func (x *TableIndex) GetMaxVersion() uint64 {
	if x != nil {
		return x.MaxVersion
	}
	return 0
}

func (x *TableIndex) GetKeyCount() uint32 {
	if x != nil {
		return x.KeyCount
	}
	return 0
}

func (x *TableIndex) GetStaleDataSize() uint32 {
	if x != nil {
		return x.StaleDataSize
	}
	return 0
}

// BlockOffset 组成 sst 文件的 block
type BlockOffset struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key    []byte `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`        // 该 block 的第一个 key
	Offset uint32 `protobuf:"varint,2,opt,name=offset,proto3" json:"offset,omitempty"` // 第一个 key 在 sst 的偏移量
	Len    uint32 `protobuf:"varint,3,opt,name=len,proto3" json:"len,omitempty"`       // 该 block 的长度
}

func (x *BlockOffset) Reset() {
	*x = BlockOffset{}
	mi := &file_pb_pb_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *BlockOffset) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BlockOffset) ProtoMessage() {}

func (x *BlockOffset) ProtoReflect() protoreflect.Message {
	mi := &file_pb_pb_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BlockOffset.ProtoReflect.Descriptor instead.
func (*BlockOffset) Descriptor() ([]byte, []int) {
	return file_pb_pb_proto_rawDescGZIP(), []int{3}
}

func (x *BlockOffset) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *BlockOffset) GetOffset() uint32 {
	if x != nil {
		return x.Offset
	}
	return 0
}

func (x *BlockOffset) GetLen() uint32 {
	if x != nil {
		return x.Len
	}
	return 0
}

type ManifestChangeSet struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Changes []*ManifestChange `protobuf:"bytes,1,rep,name=changes,proto3" json:"changes,omitempty"`
}

func (x *ManifestChangeSet) Reset() {
	*x = ManifestChangeSet{}
	mi := &file_pb_pb_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ManifestChangeSet) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ManifestChangeSet) ProtoMessage() {}

func (x *ManifestChangeSet) ProtoReflect() protoreflect.Message {
	mi := &file_pb_pb_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ManifestChangeSet.ProtoReflect.Descriptor instead.
func (*ManifestChangeSet) Descriptor() ([]byte, []int) {
	return file_pb_pb_proto_rawDescGZIP(), []int{4}
}

func (x *ManifestChangeSet) GetChanges() []*ManifestChange {
	if x != nil {
		return x.Changes
	}
	return nil
}

type ManifestChange struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id       uint64                   `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	Op       ManifestChange_Operation `protobuf:"varint,2,opt,name=op,proto3,enum=pb.ManifestChange_Operation" json:"op,omitempty"`
	Level    uint32                   `protobuf:"varint,3,opt,name=level,proto3" json:"level,omitempty"`      // 仅用于 CREATE
	Checksum []byte                   `protobuf:"bytes,4,opt,name=checksum,proto3" json:"checksum,omitempty"` // 仅用于 CREATE
}

func (x *ManifestChange) Reset() {
	*x = ManifestChange{}
	mi := &file_pb_pb_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ManifestChange) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ManifestChange) ProtoMessage() {}

func (x *ManifestChange) ProtoReflect() protoreflect.Message {
	mi := &file_pb_pb_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ManifestChange.ProtoReflect.Descriptor instead.
func (*ManifestChange) Descriptor() ([]byte, []int) {
	return file_pb_pb_proto_rawDescGZIP(), []int{5}
}

func (x *ManifestChange) GetId() uint64 {
	if x != nil {
		return x.Id
	}
	return 0
}

func (x *ManifestChange) GetOp() ManifestChange_Operation {
	if x != nil {
		return x.Op
	}
	return ManifestChange_CREATE
}

func (x *ManifestChange) GetLevel() uint32 {
	if x != nil {
		return x.Level
	}
	return 0
}

func (x *ManifestChange) GetChecksum() []byte {
	if x != nil {
		return x.Checksum
	}
	return nil
}

var File_pb_pb_proto protoreflect.FileDescriptor

var file_pb_pb_proto_rawDesc = []byte{
	0x0a, 0x0b, 0x70, 0x62, 0x2f, 0x70, 0x62, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x02, 0x70,
	0x62, 0x22, 0x79, 0x0a, 0x02, 0x4b, 0x56, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c,
	0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x12,
	0x18, 0x0a, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04,
	0x52, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x1d, 0x0a, 0x0a, 0x65, 0x78, 0x70,
	0x69, 0x72, 0x65, 0x73, 0x5f, 0x61, 0x74, 0x18, 0x04, 0x20, 0x01, 0x28, 0x04, 0x52, 0x09, 0x65,
	0x78, 0x70, 0x69, 0x72, 0x65, 0x73, 0x41, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x6d, 0x65, 0x74, 0x61,
	0x18, 0x05, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x04, 0x6d, 0x65, 0x74, 0x61, 0x22, 0x22, 0x0a, 0x06,
	0x4b, 0x56, 0x4c, 0x69, 0x73, 0x74, 0x12, 0x18, 0x0a, 0x03, 0x6b, 0x76, 0x73, 0x18, 0x01, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x06, 0x2e, 0x70, 0x62, 0x2e, 0x4b, 0x56, 0x52, 0x03, 0x6b, 0x76, 0x73,
	0x22, 0x99, 0x01, 0x0a, 0x0a, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x12,
	0x29, 0x0a, 0x07, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x0f, 0x2e, 0x70, 0x62, 0x2e, 0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x4f, 0x66, 0x66, 0x73, 0x65,
	0x74, 0x52, 0x07, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x73, 0x12, 0x1e, 0x0a, 0x0a, 0x6d, 0x61,
	0x78, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0a,
	0x6d, 0x61, 0x78, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x1a, 0x0a, 0x08, 0x6b, 0x65,
	0x79, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x08, 0x6b, 0x65,
	0x79, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x12, 0x24, 0x0a, 0x0d, 0x73, 0x74, 0x61, 0x6c, 0x65, 0x44,
	0x61, 0x74, 0x61, 0x53, 0x69, 0x7a, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x0d, 0x73,
	0x74, 0x61, 0x6c, 0x65, 0x44, 0x61, 0x74, 0x61, 0x53, 0x69, 0x7a, 0x65, 0x22, 0x49, 0x0a, 0x0b,
	0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x4f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x6b,
	0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x16, 0x0a,
	0x06, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x06, 0x6f,
	0x66, 0x66, 0x73, 0x65, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x6c, 0x65, 0x6e, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x0d, 0x52, 0x03, 0x6c, 0x65, 0x6e, 0x22, 0x41, 0x0a, 0x11, 0x4d, 0x61, 0x6e, 0x69, 0x66,
	0x65, 0x73, 0x74, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x53, 0x65, 0x74, 0x12, 0x2c, 0x0a, 0x07,
	0x63, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x12, 0x2e,
	0x70, 0x62, 0x2e, 0x4d, 0x61, 0x6e, 0x69, 0x66, 0x65, 0x73, 0x74, 0x43, 0x68, 0x61, 0x6e, 0x67,
	0x65, 0x52, 0x07, 0x63, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x73, 0x22, 0xa5, 0x01, 0x0a, 0x0e, 0x4d,
	0x61, 0x6e, 0x69, 0x66, 0x65, 0x73, 0x74, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x12, 0x0e, 0x0a,
	0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x02, 0x69, 0x64, 0x12, 0x2c, 0x0a,
	0x02, 0x6f, 0x70, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x1c, 0x2e, 0x70, 0x62, 0x2e, 0x4d,
	0x61, 0x6e, 0x69, 0x66, 0x65, 0x73, 0x74, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x2e, 0x4f, 0x70,
	0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x02, 0x6f, 0x70, 0x12, 0x14, 0x0a, 0x05, 0x6c,
	0x65, 0x76, 0x65, 0x6c, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x05, 0x6c, 0x65, 0x76, 0x65,
	0x6c, 0x12, 0x1a, 0x0a, 0x08, 0x63, 0x68, 0x65, 0x63, 0x6b, 0x73, 0x75, 0x6d, 0x18, 0x04, 0x20,
	0x01, 0x28, 0x0c, 0x52, 0x08, 0x63, 0x68, 0x65, 0x63, 0x6b, 0x73, 0x75, 0x6d, 0x22, 0x23, 0x0a,
	0x09, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x0a, 0x0a, 0x06, 0x43, 0x52,
	0x45, 0x41, 0x54, 0x45, 0x10, 0x00, 0x12, 0x0a, 0x0a, 0x06, 0x44, 0x45, 0x4c, 0x45, 0x54, 0x45,
	0x10, 0x01, 0x42, 0x05, 0x5a, 0x03, 0x2f, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x33,
}

var (
	file_pb_pb_proto_rawDescOnce sync.Once
	file_pb_pb_proto_rawDescData = file_pb_pb_proto_rawDesc
)

func file_pb_pb_proto_rawDescGZIP() []byte {
	file_pb_pb_proto_rawDescOnce.Do(func() {
		file_pb_pb_proto_rawDescData = protoimpl.X.CompressGZIP(file_pb_pb_proto_rawDescData)
	})
	return file_pb_pb_proto_rawDescData
}

var file_pb_pb_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_pb_pb_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_pb_pb_proto_goTypes = []any{
	(ManifestChange_Operation)(0), // 0: pb.ManifestChange.Operation
	(*KV)(nil),                    // 1: pb.KV
	(*KVList)(nil),                // 2: pb.KVList
	(*TableIndex)(nil),            // 3: pb.TableIndex
	(*BlockOffset)(nil),           // 4: pb.BlockOffset
	(*ManifestChangeSet)(nil),     // 5: pb.ManifestChangeSet
	(*ManifestChange)(nil),        // 6: pb.ManifestChange
}
var file_pb_pb_proto_depIdxs = []int32{
	1, // 0: pb.KVList.kvs:type_name -> pb.KV
	4, // 1: pb.TableIndex.offsets:type_name -> pb.BlockOffset
	6, // 2: pb.ManifestChangeSet.changes:type_name -> pb.ManifestChange
	0, // 3: pb.ManifestChange.op:type_name -> pb.ManifestChange.Operation
	4, // [4:4] is the sub-list for method output_type
	4, // [4:4] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() { file_pb_pb_proto_init() }
func file_pb_pb_proto_init() {
	if File_pb_pb_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_pb_pb_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_pb_pb_proto_goTypes,
		DependencyIndexes: file_pb_pb_proto_depIdxs,
		EnumInfos:         file_pb_pb_proto_enumTypes,
		MessageInfos:      file_pb_pb_proto_msgTypes,
	}.Build()
	File_pb_pb_proto = out.File
	file_pb_pb_proto_rawDesc = nil
	file_pb_pb_proto_goTypes = nil
	file_pb_pb_proto_depIdxs = nil
}
