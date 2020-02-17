// Code generated by protoc-gen-go. DO NOT EDIT.
// source: cdx.proto

package cdx

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type Cdx struct {
	Uri                  string   `protobuf:"bytes,1,opt,name=uri,proto3" json:"uri,omitempty"`
	Ref                  string   `protobuf:"bytes,2,opt,name=ref,proto3" json:"ref,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Cdx) Reset()         { *m = Cdx{} }
func (m *Cdx) String() string { return proto.CompactTextString(m) }
func (*Cdx) ProtoMessage()    {}
func (*Cdx) Descriptor() ([]byte, []int) {
	return fileDescriptor_dd71725f001d5944, []int{0}
}

func (m *Cdx) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Cdx.Unmarshal(m, b)
}
func (m *Cdx) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Cdx.Marshal(b, m, deterministic)
}
func (m *Cdx) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Cdx.Merge(m, src)
}
func (m *Cdx) XXX_Size() int {
	return xxx_messageInfo_Cdx.Size(m)
}
func (m *Cdx) XXX_DiscardUnknown() {
	xxx_messageInfo_Cdx.DiscardUnknown(m)
}

var xxx_messageInfo_Cdx proto.InternalMessageInfo

func (m *Cdx) GetUri() string {
	if m != nil {
		return m.Uri
	}
	return ""
}

func (m *Cdx) GetRef() string {
	if m != nil {
		return m.Ref
	}
	return ""
}

func init() {
	proto.RegisterType((*Cdx)(nil), "gowarc.cdx.Cdx")
}

func init() { proto.RegisterFile("cdx.proto", fileDescriptor_dd71725f001d5944) }

var fileDescriptor_dd71725f001d5944 = []byte{
	// 118 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x4c, 0x4e, 0xa9, 0xd0,
	0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0xe2, 0x4a, 0xcf, 0x2f, 0x4f, 0x2c, 0x4a, 0xd6, 0x4b, 0x4e,
	0xa9, 0x50, 0xd2, 0xe4, 0x62, 0x76, 0x4e, 0xa9, 0x10, 0x12, 0xe0, 0x62, 0x2e, 0x2d, 0xca, 0x94,
	0x60, 0x54, 0x60, 0xd4, 0xe0, 0x0c, 0x02, 0x31, 0x41, 0x22, 0x45, 0xa9, 0x69, 0x12, 0x4c, 0x10,
	0x91, 0xa2, 0xd4, 0x34, 0x27, 0xc5, 0x28, 0xf9, 0xf4, 0xcc, 0x92, 0x8c, 0xd2, 0x24, 0xbd, 0xe4,
	0xfc, 0x5c, 0xfd, 0xbc, 0x9c, 0xbc, 0xf2, 0x44, 0x7d, 0x88, 0x49, 0xfa, 0xc9, 0x29, 0x15, 0xd6,
	0xc9, 0x29, 0x15, 0x49, 0x6c, 0x60, 0x0b, 0x8c, 0x01, 0x01, 0x00, 0x00, 0xff, 0xff, 0x1f, 0x61,
	0xff, 0x9a, 0x6d, 0x00, 0x00, 0x00,
}