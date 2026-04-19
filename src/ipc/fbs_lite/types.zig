const flatbufferz = @import("fbs_runtime");
const arrow_fbs = @import("ipc_schema");

pub const PackError = flatbufferz.common.PackError;
pub const PackOptions = flatbufferz.common.PackOptions;

// Arrow IPC top-level objects covered by this lite layer.
pub const Message = arrow_fbs.org_apache_arrow_flatbuf_Message.Message;
pub const MessageT = arrow_fbs.org_apache_arrow_flatbuf_Message.MessageT;
pub const MessageHeader = arrow_fbs.org_apache_arrow_flatbuf_MessageHeader.MessageHeader;
pub const MetadataVersion = arrow_fbs.org_apache_arrow_flatbuf_MetadataVersion.MetadataVersion;

pub const Schema = arrow_fbs.org_apache_arrow_flatbuf_Schema.Schema;
pub const SchemaT = arrow_fbs.org_apache_arrow_flatbuf_Schema.SchemaT;
pub const FieldT = arrow_fbs.org_apache_arrow_flatbuf_Field.FieldT;

pub const RecordBatchT = arrow_fbs.org_apache_arrow_flatbuf_RecordBatch.RecordBatchT;
pub const DictionaryBatchT = arrow_fbs.org_apache_arrow_flatbuf_DictionaryBatch.DictionaryBatchT;

pub const Footer = arrow_fbs.org_apache_arrow_flatbuf_Footer.Footer;
pub const FooterT = arrow_fbs.org_apache_arrow_flatbuf_Footer.FooterT;
pub const BlockT = arrow_fbs.org_apache_arrow_flatbuf_Block.BlockT;
pub const KeyValueT = arrow_fbs.org_apache_arrow_flatbuf_KeyValue.KeyValueT;

pub const TensorT = arrow_fbs.org_apache_arrow_flatbuf_Tensor.TensorT;
pub const SparseTensorT = arrow_fbs.org_apache_arrow_flatbuf_SparseTensor.SparseTensorT;
