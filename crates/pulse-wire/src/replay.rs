use serde::{Deserialize, Serialize};

use crate::batch::{
    decode_zstd_bounded, ByteCursor, FLAG_RAW_ZSTD, MAX_DECOMPRESSED_BYTES, WIRE_VERSION,
};
use crate::error::{Result, WireError};

pub const REPLAY_MAGIC: [u8; 4] = *b"PLSR";
pub const MASK_CHAR: char = '█';
pub const FIXED_MASK_PLACEHOLDER: &str = "████";
pub const MAX_REPLAY_OPS_PER_CHUNK: usize = 100_000;
pub const MAX_SNAPSHOT_NODES: usize = 50_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeKind {
    Screen,
    Container,
    Button,
    MaskedInput,
    Text,
    ImagePlaceholder,
    ErrorBanner,
}

impl NodeKind {
    pub fn to_tag(self) -> u8 {
        match self {
            NodeKind::Screen => 0,
            NodeKind::Container => 1,
            NodeKind::Button => 2,
            NodeKind::MaskedInput => 3,
            NodeKind::Text => 4,
            NodeKind::ImagePlaceholder => 5,
            NodeKind::ErrorBanner => 6,
        }
    }

    pub fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            0 => Ok(NodeKind::Screen),
            1 => Ok(NodeKind::Container),
            2 => Ok(NodeKind::Button),
            3 => Ok(NodeKind::MaskedInput),
            4 => Ok(NodeKind::Text),
            5 => Ok(NodeKind::ImagePlaceholder),
            6 => Ok(NodeKind::ErrorBanner),
            other => Err(WireError::InvalidReplayTag(other)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireNode {
    pub node_id: u32,
    pub parent_id: u32,
    pub kind: NodeKind,
    pub x: u16,
    pub y: u16,
    pub w: u16,
    pub h: u16,
    pub bg_rgba: u32,
    pub masked_label: String,
}

impl WireNode {
    /// Enforces privacy-by-design masking on `MaskedInput` nodes and obvious PII tokens.
    pub fn sanitized(mut self) -> Self {
        if self.kind == NodeKind::MaskedInput {
            self.masked_label = mask_input_value(&self.masked_label);
        }
        self
    }

    fn write_to(&self, out: &mut Vec<u8>) {
        let sanitized_label = if self.kind == NodeKind::MaskedInput {
            mask_input_value(&self.masked_label)
        } else {
            self.masked_label.clone()
        };
        out.extend_from_slice(&self.node_id.to_le_bytes());
        out.extend_from_slice(&self.parent_id.to_le_bytes());
        out.push(self.kind.to_tag());
        out.extend_from_slice(&self.x.to_le_bytes());
        out.extend_from_slice(&self.y.to_le_bytes());
        out.extend_from_slice(&self.w.to_le_bytes());
        out.extend_from_slice(&self.h.to_le_bytes());
        out.extend_from_slice(&self.bg_rgba.to_le_bytes());
        write_str(out, &sanitized_label);
    }

    fn read_from(cursor: &mut ByteCursor<'_>) -> Result<Self> {
        let node_id = cursor.read_u32("node_id")?;
        let parent_id = cursor.read_u32("parent_id")?;
        let kind = NodeKind::from_tag(cursor.read_u8("node_kind")?)?;
        let x = cursor.read_u16("node.x")?;
        let y = cursor.read_u16("node.y")?;
        let w = cursor.read_u16("node.w")?;
        let h = cursor.read_u16("node.h")?;
        let bg_rgba = cursor.read_u32("node.bg_rgba")?;
        let masked_label = cursor.read_string("node.masked_label")?;
        Ok(WireNode {
            node_id,
            parent_id,
            kind,
            x,
            y,
            w,
            h,
            bg_rgba,
            masked_label,
        })
    }
}

/// Masks sensitive text or input values to a constant-length `"████"` placeholder (`P3-01`),
/// never preserving prefix labels or character counts that could leak PII.
pub fn mask_input_value(_raw: &str) -> String {
    FIXED_MASK_PLACEHOLDER.to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InteractionKind {
    Tap,
    RageTap { count: u8 },
    DeadTap,
    Scroll,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum WireframeOp {
    FullSnapshot {
        width: u16,
        height: u16,
        nodes: Vec<WireNode>,
    },
    NodeUpsert(WireNode),
    NodeRemove {
        node_id: u32,
    },
    PointerInteraction {
        ts_offset_ms: u32,
        x: u16,
        y: u16,
        kind: InteractionKind,
    },
    NetworkBreadcrumb {
        ts_offset_ms: u32,
        method: String,
        path: String,
        status_code: u16,
        duration_ms: u32,
    },
}

impl WireframeOp {
    const OP_FULL_SNAPSHOT: u8 = 1;
    const OP_NODE_UPSERT: u8 = 2;
    const OP_NODE_REMOVE: u8 = 3;
    const OP_POINTER: u8 = 4;
    const OP_NETWORK: u8 = 5;

    fn write_to(&self, out: &mut Vec<u8>) {
        match self {
            WireframeOp::FullSnapshot {
                width,
                height,
                nodes,
            } => {
                out.push(Self::OP_FULL_SNAPSHOT);
                out.extend_from_slice(&width.to_le_bytes());
                out.extend_from_slice(&height.to_le_bytes());
                out.extend_from_slice(&(nodes.len() as u32).to_le_bytes());
                for node in nodes {
                    node.write_to(out);
                }
            }
            WireframeOp::NodeUpsert(node) => {
                out.push(Self::OP_NODE_UPSERT);
                node.write_to(out);
            }
            WireframeOp::NodeRemove { node_id } => {
                out.push(Self::OP_NODE_REMOVE);
                out.extend_from_slice(&node_id.to_le_bytes());
            }
            WireframeOp::PointerInteraction {
                ts_offset_ms,
                x,
                y,
                kind,
            } => {
                out.push(Self::OP_POINTER);
                out.extend_from_slice(&ts_offset_ms.to_le_bytes());
                out.extend_from_slice(&x.to_le_bytes());
                out.extend_from_slice(&y.to_le_bytes());
                match kind {
                    InteractionKind::Tap => out.push(0),
                    InteractionKind::RageTap { count } => {
                        out.push(1);
                        out.push(*count);
                    }
                    InteractionKind::DeadTap => out.push(2),
                    InteractionKind::Scroll => out.push(3),
                }
            }
            WireframeOp::NetworkBreadcrumb {
                ts_offset_ms,
                method,
                path,
                status_code,
                duration_ms,
            } => {
                out.push(Self::OP_NETWORK);
                out.extend_from_slice(&ts_offset_ms.to_le_bytes());
                write_str(out, method);
                write_str(out, path);
                out.extend_from_slice(&status_code.to_le_bytes());
                out.extend_from_slice(&duration_ms.to_le_bytes());
            }
        }
    }

    fn read_from(cursor: &mut ByteCursor<'_>) -> Result<Self> {
        let op = cursor.read_u8("wireframe op")?;
        match op {
            Self::OP_FULL_SNAPSHOT => {
                let width = cursor.read_u16("snapshot.width")?;
                let height = cursor.read_u16("snapshot.height")?;
                let count = cursor.read_u32("snapshot.nodes.len")? as usize;
                if count > MAX_SNAPSHOT_NODES {
                    return Err(WireError::ResourceLimitExceeded(
                        "replay snapshot node count exceeds MAX_SNAPSHOT_NODES",
                    ));
                }
                // Minimum wire size of a WireNode is 4+4+1+2+2+2+2+4+4 = 25 bytes
                let mut nodes = Vec::with_capacity(cursor.bounded_capacity(count, 25));
                for _ in 0..count {
                    nodes.push(WireNode::read_from(cursor)?);
                }
                Ok(WireframeOp::FullSnapshot {
                    width,
                    height,
                    nodes,
                })
            }
            Self::OP_NODE_UPSERT => Ok(WireframeOp::NodeUpsert(WireNode::read_from(cursor)?)),
            Self::OP_NODE_REMOVE => Ok(WireframeOp::NodeRemove {
                node_id: cursor.read_u32("remove.node_id")?,
            }),
            Self::OP_POINTER => {
                let ts_offset_ms = cursor.read_u32("pointer.ts")?;
                let x = cursor.read_u16("pointer.x")?;
                let y = cursor.read_u16("pointer.y")?;
                let k = cursor.read_u8("pointer.kind")?;
                let kind = match k {
                    0 => InteractionKind::Tap,
                    1 => InteractionKind::RageTap {
                        count: cursor.read_u8("pointer.rage_count")?,
                    },
                    2 => InteractionKind::DeadTap,
                    3 => InteractionKind::Scroll,
                    other => return Err(WireError::InvalidReplayTag(other)),
                };
                Ok(WireframeOp::PointerInteraction {
                    ts_offset_ms,
                    x,
                    y,
                    kind,
                })
            }
            Self::OP_NETWORK => {
                let ts_offset_ms = cursor.read_u32("net.ts")?;
                let method = cursor.read_string("net.method")?;
                let path = cursor.read_string("net.path")?;
                let status_code = cursor.read_u16("net.status")?;
                let duration_ms = cursor.read_u32("net.duration")?;
                Ok(WireframeOp::NetworkBreadcrumb {
                    ts_offset_ms,
                    method,
                    path,
                    status_code,
                    duration_ms,
                })
            }
            other => Err(WireError::InvalidReplayTag(other)),
        }
    }
}

/// Self-framing binary/JSON Wireframe Replay Chunk (`PLSR`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayChunk {
    pub project_id: String,
    pub session_id: String,
    pub chunk_start_ts_ms: i64,
    pub ops: Vec<WireframeOp>,
}

impl ReplayChunk {
    /// Encodes a self-framing `PLSR` chunk:
    /// `PLSR (4B) | version (1B) | flags (1B) | compressed_len (4B LE) | zstd_payload`.
    pub fn encode_binary(&self, zstd_level: i32) -> Result<Vec<u8>> {
        let mut raw = Vec::with_capacity(128 + self.ops.len() * 32);
        write_str(&mut raw, &self.project_id);
        write_str(&mut raw, &self.session_id);
        raw.extend_from_slice(&self.chunk_start_ts_ms.to_le_bytes());
        raw.extend_from_slice(&(self.ops.len() as u32).to_le_bytes());
        for op in &self.ops {
            op.write_to(&mut raw);
        }

        let compressed = zstd::stream::encode_all(raw.as_slice(), zstd_level)?;
        let mut frame = Vec::with_capacity(10 + compressed.len());
        frame.extend_from_slice(&REPLAY_MAGIC);
        frame.push(WIRE_VERSION);
        frame.push(FLAG_RAW_ZSTD);
        frame.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
        frame.extend_from_slice(&compressed);
        Ok(frame)
    }

    /// Decodes a single `PLSR` chunk from the beginning of `bytes`, returning `(chunk, bytes_consumed)`.
    pub fn decode_frame_prefix(bytes: &[u8]) -> Result<(Self, usize)> {
        if bytes.len() < 10 {
            return Err(WireError::TruncatedFrame("PLSR header < 10 bytes"));
        }
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&bytes[0..4]);
        if magic != REPLAY_MAGIC {
            return Err(WireError::InvalidMagic {
                expected: REPLAY_MAGIC,
                actual: magic,
            });
        }
        let version = bytes[4];
        if version != WIRE_VERSION {
            return Err(WireError::UnsupportedVersion(version));
        }
        let flags = bytes[5];
        if flags != FLAG_RAW_ZSTD {
            return Err(WireError::UnsupportedFlags(flags));
        }
        let payload_len = u32::from_le_bytes([bytes[6], bytes[7], bytes[8], bytes[9]]) as usize;
        let frame_end = 10usize
            .checked_add(payload_len)
            .ok_or(WireError::TruncatedFrame("PLSR frame overflow"))?;
        if bytes.len() < frame_end {
            return Err(WireError::TruncatedFrame("PLSR payload truncated"));
        }

        let decompressed =
            decode_zstd_bounded(&bytes[10..frame_end], None, MAX_DECOMPRESSED_BYTES)?;
        let mut cursor = ByteCursor::new(&decompressed);
        let project_id = cursor.read_string("chunk.project_id")?;
        let session_id = cursor.read_string("chunk.session_id")?;
        let chunk_start_ts_ms = cursor.read_i64("chunk.start_ts")?;
        let op_count = cursor.read_u32("chunk.ops.len")? as usize;
        if op_count > MAX_REPLAY_OPS_PER_CHUNK {
            return Err(WireError::ResourceLimitExceeded(
                "replay chunk op count exceeds MAX_REPLAY_OPS_PER_CHUNK",
            ));
        }
        // Minimum wire size of any WireframeOp is 1 (tag) + 4 (node_id) = 5 bytes
        let mut ops = Vec::with_capacity(cursor.bounded_capacity(op_count, 5));
        for _ in 0..op_count {
            ops.push(WireframeOp::read_from(&mut cursor)?);
        }

        let sanitized_ops = ops
            .into_iter()
            .map(|op| match op {
                WireframeOp::FullSnapshot {
                    width,
                    height,
                    nodes,
                } => WireframeOp::FullSnapshot {
                    width,
                    height,
                    nodes: nodes.into_iter().map(WireNode::sanitized).collect(),
                },
                WireframeOp::NodeUpsert(n) => WireframeOp::NodeUpsert(n.sanitized()),
                other => other,
            })
            .collect();

        Ok((
            ReplayChunk {
                project_id,
                session_id,
                chunk_start_ts_ms,
                ops: sanitized_ops,
            },
            frame_end,
        ))
    }

    pub fn decode_binary(bytes: &[u8]) -> Result<Self> {
        let (chunk, _) = Self::decode_frame_prefix(bytes)?;
        Ok(chunk)
    }

    /// Decodes a concatenated `.plsr` stream (even when byte-range sliced mid-file by scanning for `PLSR` magic).
    pub fn decode_stream(bytes: &[u8]) -> Result<Vec<Self>> {
        let mut pos = 0usize;
        // If a byte-range read landed slightly before a `PLSR` frame boundary, scan forward to the first valid magic header.
        if bytes.len() >= 4 && bytes[0..4] != REPLAY_MAGIC {
            if let Some(found) = bytes.windows(4).position(|w| w == REPLAY_MAGIC) {
                pos = found;
            }
        }
        let mut chunks = Vec::new();
        while pos < bytes.len() {
            let (chunk, consumed) = Self::decode_frame_prefix(&bytes[pos..])?;
            chunks.push(chunk);
            pos += consumed;
        }
        Ok(chunks)
    }

    pub fn decode_auto(bytes: &[u8], content_type: Option<&str>) -> Result<Self> {
        let is_binary = match content_type {
            Some(ct)
                if ct.contains("application/x-pulse-frame")
                    || ct.contains("application/octet-stream") =>
            {
                true
            }
            Some(ct) if ct.contains("application/json") => false,
            _ => bytes.len() >= 4 && bytes[0..4] == REPLAY_MAGIC,
        };
        if is_binary {
            Self::decode_binary(bytes)
        } else {
            let mut chunk: ReplayChunk = serde_json::from_slice(bytes)?;
            chunk.ops = chunk
                .ops
                .into_iter()
                .map(|op| match op {
                    WireframeOp::FullSnapshot {
                        width,
                        height,
                        nodes,
                    } => WireframeOp::FullSnapshot {
                        width,
                        height,
                        nodes: nodes.into_iter().map(WireNode::sanitized).collect(),
                    },
                    WireframeOp::NodeUpsert(n) => WireframeOp::NodeUpsert(n.sanitized()),
                    other => other,
                })
                .collect();
            Ok(chunk)
        }
    }

    pub fn has_rage_tap(&self) -> bool {
        self.ops.iter().any(|op| {
            matches!(
                op,
                WireframeOp::PointerInteraction {
                    kind: InteractionKind::RageTap { .. },
                    ..
                }
            )
        })
    }

    pub fn has_dead_tap(&self) -> bool {
        self.ops.iter().any(|op| {
            matches!(
                op,
                WireframeOp::PointerInteraction {
                    kind: InteractionKind::DeadTap,
                    ..
                }
            )
        })
    }

    pub fn has_error(&self) -> bool {
        self.ops.iter().any(|op| match op {
            WireframeOp::NetworkBreadcrumb { status_code, .. } => *status_code >= 500,
            WireframeOp::FullSnapshot { nodes, .. } => {
                nodes.iter().any(|n| n.kind == NodeKind::ErrorBanner)
            }
            WireframeOp::NodeUpsert(n) => n.kind == NodeKind::ErrorBanner,
            _ => false,
        })
    }
}

#[inline]
fn write_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}
