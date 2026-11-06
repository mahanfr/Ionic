//! Deterministic, self-describing byte encoding for the wire.
//! Nothing in `ionic` needs to derive serde — we encode the public
//! fields manually and re-hydrate them on the other side.

use ethnum::u256;
use ionic::{
    blocks::{Block, BlockHeader},
    transactions::Transaction,
    types::{BlockPos, IonicAddr, IonicHash, IonicPK},
};

pub const MAX_WIRE_SIZE: usize = 8 * 1024 * 1024;

#[derive(Debug)]
pub enum WireError {
    Eof,
    Malformed(&'static str),
}
impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WireError::Eof => write!(f, "unexpected end of input"),
            WireError::Malformed(s) => write!(f, "malformed message: {s}"),
        }
    }
}
impl std::error::Error for WireError {}

struct Cursor<'a> { bytes: &'a [u8], pos: usize }
impl<'a> Cursor<'a> {
    fn new(b: &'a [u8]) -> Self { Self { bytes: b, pos: 0 } }
    fn remaining(&self) -> usize { self.bytes.len().saturating_sub(self.pos) }
    fn take(&mut self, n: usize) -> Result<&'a [u8], WireError> {
        let end = self.pos.checked_add(n).ok_or(WireError::Eof)?;
        let s = self.bytes.get(self.pos..end).ok_or(WireError::Eof)?;
        self.pos = end;
        Ok(s)
    }
    fn u8(&mut self)   -> Result<u8,   WireError> { Ok(self.take(1)?[0]) }
    fn u32(&mut self)  -> Result<u32,  WireError> { Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap())) }
    fn u64(&mut self)  -> Result<u64,  WireError> { Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap())) }
    fn h32(&mut self)  -> Result<[u8;32], WireError> { Ok(self.take(32)?.try_into().unwrap()) }
    fn h64(&mut self)  -> Result<[u8;64], WireError> { Ok(self.take(64)?.try_into().unwrap()) }
    fn u256(&mut self) -> Result<u256, WireError> { Ok(u256::from_le_bytes(self.h32()?)) }
    fn vec_u8(&mut self) -> Result<Vec<u8>, WireError> {
        let len = self.u32()? as usize;
        if len > MAX_WIRE_SIZE || len > self.remaining() {
            return Err(WireError::Malformed("oversized vector"));
        }
        Ok(self.take(len)?.to_vec())
    }
}

// ---------- block header ----------
fn encode_header(h: &BlockHeader, out: &mut Vec<u8>) {
    out.push(h.version);
    out.extend_from_slice(&h.chain_id.to_le_bytes());
    out.extend_from_slice(&h.index.to_le_bytes());
    out.extend_from_slice(&h.position.height.to_le_bytes());
    out.extend_from_slice(&h.position.round.to_le_bytes());
    out.extend_from_slice(h.previous_hash.as_ref());
    out.extend_from_slice(&h.timestamp.to_le_bytes());
    out.extend_from_slice(h.proposer.as_ref());
    out.extend_from_slice(h.transactions_root.as_ref());
    out.extend_from_slice(h.state_root.as_ref());
    out.extend_from_slice(&h.gas_limit.to_le_bytes());
    out.extend_from_slice(&h.gas_used.to_le_bytes());
    out.extend_from_slice(&h.base_fee.to_le_bytes());
}
fn decode_header(c: &mut Cursor) -> Result<BlockHeader, WireError> {
    let version  = c.u8()?;
    let chain_id = c.u64()?;
    let index    = c.u64()?;
    let height   = c.u64()?;
    let round    = c.u64()?;
    let prev     = IonicHash::from(c.h32()?);
    let ts       = c.u64()?;
    let proposer = IonicPK::from(c.h32()?);
    let tx_root  = IonicHash::from(c.h32()?);
    let st_root  = IonicHash::from(c.h32()?);
    let gas_lim  = c.u64()?;
    let gas_use  = c.u64()?;
    let base_fee = c.u256()?;
    Ok(BlockHeader {
        version, chain_id, index,
        position: BlockPos::new(height, round),
        previous_hash: prev, timestamp: ts, proposer,
        transactions_root: tx_root, state_root: st_root,
        gas_limit: gas_lim, gas_used: gas_use, base_fee,
    })
}

// ---------- transaction ----------
fn encode_tx(tx: &Transaction, out: &mut Vec<u8>) {
    out.push(tx.version);
    out.extend_from_slice(&tx.chain_id.to_le_bytes());
    out.extend_from_slice(&tx.nonce.to_le_bytes());
    out.extend_from_slice(tx.sender_pk.as_ref());
    match tx.recepient {
        Some(a) => { out.push(1); out.extend_from_slice(a.as_ref()); }
        None    => { out.push(0); }
    }
    out.extend_from_slice(&tx.value.to_le_bytes());
    out.extend_from_slice(&tx.gas_limit.to_le_bytes());
    out.extend_from_slice(&tx.max_fee.to_le_bytes());
    out.extend_from_slice(&tx.max_priority_fee.to_le_bytes());
    out.extend_from_slice(&tx.timestamp.to_le_bytes());
    out.extend_from_slice(&(tx.data.len() as u32).to_le_bytes());
    out.extend_from_slice(&tx.data);
    out.extend_from_slice(&tx.signature);
}
fn decode_tx(c: &mut Cursor) -> Result<Transaction, WireError> {
    let version  = c.u8()?;
    let chain_id = c.u64()?;
    let nonce    = c.u64()?;
    let sender_pk = IonicPK::from(c.h32()?);
    let recepient = match c.u8()? {
        0 => None,
        1 => Some(IonicAddr::from(c.h32()?)),
        _ => return Err(WireError::Malformed("bad option tag")),
    };
    let value     = c.u256()?;
    let gas_limit = c.u64()?;
    let max_fee   = c.u256()?;
    let max_prio  = c.u256()?;
    let timestamp = c.u64()?;
    let data      = c.vec_u8()?;
    let signature = c.h64()?;
    Ok(Transaction {
        version, chain_id, nonce, sender_pk, recepient,
        value, gas_limit, max_fee, max_priority_fee: max_prio,
        timestamp, data, signature,
    })
}

// ---------- public block / tx API ----------
pub fn encode_block(b: &Block) -> Vec<u8> {
    let mut out = Vec::new();
    encode_header(&b.header, &mut out);
    out.extend_from_slice(&(b.transactions.len() as u32).to_le_bytes());
    for tx in &b.transactions { encode_tx(tx, &mut out); }
    out.extend_from_slice(&b.signature);
    out
}
pub fn decode_block(bytes: &[u8]) -> Result<Block, WireError> {
    let mut c = Cursor::new(bytes);
    let header = decode_header(&mut c)?;
    let n = c.u32()? as usize;
    if n > MAX_WIRE_SIZE { return Err(WireError::Malformed("too many txs")); }
    let mut txs = Vec::with_capacity(n.min(4096));
    for _ in 0..n { txs.push(decode_tx(&mut c)?); }
    let signature = c.h64()?;
    Ok(Block { header, transactions: txs, signature })
}
pub fn encode_transaction(tx: &Transaction) -> Vec<u8> {
    let mut out = Vec::new(); encode_tx(tx, &mut out); out
}
pub fn decode_transaction(bytes: &[u8]) -> Result<Transaction, WireError> {
    decode_tx(&mut Cursor::new(bytes))
}

// ---------- request / response for sync ----------
#[derive(Debug, Clone)]
pub enum SyncRequest {
    Status,
    BlockByIndex(u64),
    BlockByHash(IonicHash),
}
#[derive(Debug, Clone)]
pub enum SyncResponse {
    Status { chain_id: u64, height: u64, head: IonicHash },
    Block(Option<Block>),
}

pub fn encode_sync_request(r: &SyncRequest) -> Vec<u8> {
    let mut out = Vec::new();
    match r {
        SyncRequest::Status            => out.push(0),
        SyncRequest::BlockByIndex(i)   => { out.push(1); out.extend_from_slice(&i.to_le_bytes()); }
        SyncRequest::BlockByHash(h)    => { out.push(2); out.extend_from_slice(h.as_ref()); }
    }
    out
}
pub fn decode_sync_request(b: &[u8]) -> Result<SyncRequest, WireError> {
    let mut c = Cursor::new(b);
    match c.u8()? {
        0 => Ok(SyncRequest::Status),
        1 => Ok(SyncRequest::BlockByIndex(c.u64()?)),
        2 => Ok(SyncRequest::BlockByHash(IonicHash::from(c.h32()?))),
        _ => Err(WireError::Malformed("unknown request")),
    }
}
pub fn encode_sync_response(r: &SyncResponse) -> Vec<u8> {
    let mut out = Vec::new();
    match r {
        SyncResponse::Status { chain_id, height, head } => {
            out.push(0);
            out.extend_from_slice(&chain_id.to_le_bytes());
            out.extend_from_slice(&height.to_le_bytes());
            out.extend_from_slice(head.as_ref());
        }
        SyncResponse::Block(Some(b)) => {
            out.push(1);
            let bytes = encode_block(b);
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(&bytes);
        }
        SyncResponse::Block(None) => out.push(2),
    }
    out
}
pub fn decode_sync_response(b: &[u8]) -> Result<SyncResponse, WireError> {
    let mut c = Cursor::new(b);
    match c.u8()? {
        0 => Ok(SyncResponse::Status {
            chain_id: c.u64()?, height: c.u64()?, head: IonicHash::from(c.h32()?),
        }),
        1 => {
            let len = c.u32()? as usize;
            if len > MAX_WIRE_SIZE { return Err(WireError::Malformed("block too large")); }
            Ok(SyncResponse::Block(Some(decode_block(c.take(len)?)?)))
        }
        2 => Ok(SyncResponse::Block(None)),
        _ => Err(WireError::Malformed("unknown response")),
    }
}
