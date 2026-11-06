use std::{io, time::Duration};
use anyhow::{anyhow, Result};
use futures::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    StreamExt,
};
use libp2p::{
    gossipsub, identify, identity::Keypair, kad, noise, ping, request_response,
    swarm::NetworkBehaviour, tcp, yamux, PeerId, StreamProtocol, Swarm, SwarmBuilder,
};
use crate::messages::{
    self, SyncRequest, SyncResponse, WireError, MAX_WIRE_SIZE,
};

pub const PROTOCOL_NAME: &str = "/ionic/1.0.0";
pub const SYNC_PROTOCOL: &str = "/ionic/sync/1";

// ---------- libp2p behaviour ----------
#[derive(NetworkBehaviour)]
pub struct IonicBehaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub identify:  identify::Behaviour,
    pub kademlia:  kad::Behaviour<kad::store::MemoryStore>,
    pub ping:      ping::Behaviour,
    pub sync:      request_response::Behaviour<IonicSyncCodec>,
}

// ---------- custom codec ----------
#[derive(Debug, Clone)]
pub struct IonicSyncCodec;

async fn read_framed<T: AsyncRead + Unpin + Send>(io: &mut T) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    io.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_WIRE_SIZE {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "message too large"));
    }
    let mut buf = vec![0u8; len];
    io.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn write_framed<T: AsyncWrite + Unpin + Send>(io: &mut T, bytes: &[u8]) -> io::Result<()> {
    if bytes.len() > MAX_WIRE_SIZE {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "message too large"));
    }
    io.write_all(&(bytes.len() as u32).to_le_bytes()).await?;
    io.write_all(bytes).await?;
    io.close().await?;
    Ok(())
}

impl request_response::Codec for IonicSyncCodec {
    type Protocol = StreamProtocol;
    type Request  = SyncRequest;
    type Response = SyncResponse;

    fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T)
        -> impl Future<Output = Result<SyncRequest, io::Error>> + Send
    where T: AsyncRead + Unpin + Send {
        async move {
            let bytes = read_framed(io).await?;
            messages::decode_sync_request(&bytes)
                .map_err(|e: WireError| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
        }
    }
    fn read_response<T>(&mut self, _: &Self::Protocol, io: &mut T)
        -> impl Future<Output = Result<SyncResponse, io::Error>> + Send
    where T: AsyncRead + Unpin + Send {
        async move {
            let bytes = read_framed(io).await?;
            messages::decode_sync_response(&bytes)
                .map_err(|e: WireError| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
        }
    }
    fn write_request<T>(&mut self, _: &Self::Protocol, io: &mut T, req: SyncRequest)
        -> impl Future<Output = Result<(), io::Error>> + Send
    where T: AsyncWrite + Unpin + Send {
        async move { write_framed(io, &messages::encode_sync_request(&req)).await }
    }
    fn write_response<T>(&mut self, _: &Self::Protocol, io: &mut T, resp: SyncResponse)
        -> impl Future<Output = Result<(), io::Error>> + Send
    where T: AsyncWrite + Unpin + Send {
        async move { write_framed(io, &messages::encode_sync_response(&resp)).await }
    }
}

// ---------- build swarm ----------
pub fn build_swarm(keypair: Keypair, chain_id: u64)
    -> Result<Swarm<IonicBehaviour>>
{
    let peer_id = keypair.public().to_peer_id();

    let swarm = SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(move |key| build_behaviour(key, peer_id, chain_id))
        .map_err(|e| anyhow!("behaviour: {e}"))?
        .with_swarm_config(|cfg| {
            cfg.with_idle_connection_timeout(Duration::from_secs(300))
        })
        .build();

    Ok(swarm)
}

fn build_behaviour(
    key: &Keypair,
    peer_id: PeerId,
    _chain_id: u64,
) -> Result<IonicBehaviour, Box<dyn std::error::Error + Send + Sync>> {
    // -- gossipsub --
    let gs_cfg = gossipsub::ConfigBuilder::default()
        .heartbeat_interval(Duration::from_secs(10))
        .validation_mode(gossipsub::ValidationMode::Strict)
        .message_id_fn(|message| {
            use std::hash::{Hash, Hasher};
            let mut s = std::collections::hash_map::DefaultHasher::new();
            message.data.hash(&mut s);
            gossipsub::MessageId::from(s.finish().to_string())
        })
        .build()
        .map_err(|e| format!("gossipsub config: {e}"))?;
    let gossipsub = gossipsub::Behaviour::new(
        gossipsub::MessageAuthenticity::Signed(key.clone()),
        gs_cfg,
    )
    .map_err(|e| format!("gossipsub behaviour: {e}"))?;

    // -- identify --
    let identify = identify::Behaviour::new(identify::Config::new(
        PROTOCOL_NAME.into(),
        key.public(),
    ));

    // -- kademlia --
    let mut kademlia = kad::Behaviour::new(
        peer_id,
        kad::store::MemoryStore::new(peer_id),
    );
    kademlia.set_mode(Some(kad::Mode::Server));

    // -- ping --
    let ping = ping::Behaviour::new(ping::Config::new());

    // -- request/response --
    let sync = request_response::Behaviour::with_codec(
        IonicSyncCodec,
        [(StreamProtocol::new(SYNC_PROTOCOL), request_response::ProtocolSupport::Full)],
        request_response::Config::default(),
    );

    Ok(IonicBehaviour { gossipsub, identify, kademlia, ping, sync })
}

/// Drain and process swarm events; the caller owns node state.
pub async fn drive_event<S>(
    _swarm: &mut S,
) {}
