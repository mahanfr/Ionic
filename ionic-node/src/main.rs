mod messages;
mod network;
mod node;

use std::time::Duration;

use anyhow::{anyhow, Result};
use futures::StreamExt;
use libp2p::{
    gossipsub, identity::Keypair, request_response, swarm::SwarmEvent,
    Multiaddr, Swarm,
};
use tokio::sync::mpsc;
use clap::{Parser, Subcommand};

use crate::{
    messages::{
        decode_block, decode_transaction, encode_block, encode_transaction,
        SyncRequest, SyncResponse,
    },
    network::{build_swarm, IonicBehaviour, IonicBehaviourEvent},
    node::Node,
};

// ---------- CLI ----------
#[derive(Parser)]
#[command(name = "ionic-node", version, about = "Ionic P2P full node")]
struct Cli { #[command(subcommand)] cmd: Cmd }

#[derive(Subcommand)]
enum Cmd {
    /// Run a full node.
    Run {
        #[arg(long, default_value_t = 1)]
        chain_id: u64,
        #[arg(long, default_value = "/ip4/127.0.0.1/tcp/0")]
        listen: Multiaddr,
        /// e.g. /ip4/127.0.0.1/tcp/4001/p2p/<PEER_ID>
        #[arg(long)]
        bootstrap: Vec<Multiaddr>,
        /// Produce an empty block every 5 s (demo / testing).
        #[arg(long, default_value_t = false)]
        mine: bool,
    },
}

enum NodeCmd { Mine, Shutdown }

// ---------- main ----------
#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info"),
    ).init();

    let cli = Cli::parse();
    match cli.cmd { Cmd::Run { chain_id, listen, bootstrap, mine } =>
        run(chain_id, listen, bootstrap, mine).await,
    }
}

async fn run(
    chain_id: u64,
    listen: Multiaddr,
    bootstrap: Vec<Multiaddr>,
    mine: bool,
) -> Result<()> {
    // 1. network identity
    let keypair = Keypair::generate_ed25519();
    let peer_id = keypair.public().to_peer_id();
    log::info!("node peer id = {peer_id}");
    log::info!("chain_id    = {chain_id}");

    // 2. swarm
    let mut swarm = build_swarm(keypair, chain_id)?;
    swarm
        .listen_on(listen.clone())
        .map_err(|e| anyhow!("listen_on {listen}: {e:?}"))?;
    for addr in &bootstrap {
        swarm
            .dial(addr.clone())
            .map_err(|e| anyhow!("dial {addr}: {e:?}"))?;
    }

    // 3. topics
    let block_topic = gossipsub::IdentTopic::new(format!("ionic/blocks/{chain_id}"));
    let tx_topic    = gossipsub::IdentTopic::new(format!("ionic/transactions/{chain_id}"));
    swarm.behaviour_mut().gossipsub.subscribe(&block_topic)?;
    swarm.behaviour_mut().gossipsub.subscribe(&tx_topic)?;

    // 4. node state
    let mut node = Node::new(chain_id);

    // 5. proposer key for demo blocks
    let (proposer_sk, proposer_vk) = ionic::keygen::generate_key_pair();
    let proposer_pk: ionic::types::IonicPK = proposer_vk.into();
    log::info!("proposer address = {}", ionic::types::IonicAddr::from_pk(&proposer_pk));

    // 6. optional miner task
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<NodeCmd>(16);
    if mine {
        let cmd_tx = cmd_tx.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            tick.tick().await; // first tick immediate
            loop {
                tick.tick().await;
                if cmd_tx.send(NodeCmd::Mine).await.is_err() { break; }
            }
        });
    }

    // 7. event loop
    loop {
        tokio::select! {
            ev = swarm.next() => {
                let Some(event) = ev else { break };
                if let Err(e) = on_swarm_event(
                    &mut node, &mut swarm, event, &block_topic, &tx_topic,
                ).await {
                    log::warn!("swarm event error: {e:#}");
                }
            }
            Some(cmd) = cmd_rx.recv() => match cmd {
                NodeCmd::Mine => {
                    if let Some(block) = node.mine_empty(proposer_pk, &proposer_sk) {
                        let h = block.hash();
                        match node.accept_block(block.clone()) {
                            Ok(()) => {
                                let bytes = encode_block(&block);
                                match swarm.behaviour_mut().gossipsub
                                    .publish(block_topic.clone(), bytes)
                                {
                                    Ok(_)  => log::info!("mined & broadcast block #{:?}", h),
                                    Err(e) => log::warn!("gossip publish failed: {e}"),
                                }
                            }
                            Err(e) => log::warn!("mined block failed validation: {e}"),
                        }
                    }
                }
                NodeCmd::Shutdown => break,
            }
        }
    }
    Ok(())
}

// ---------- event handling ----------
async fn on_swarm_event(
    node: &mut Node,
    swarm: &mut Swarm<IonicBehaviour>,
    event: SwarmEvent<IonicBehaviourEvent>,
    block_topic: &gossipsub::IdentTopic,
    tx_topic:    &gossipsub::IdentTopic,
) -> Result<()> {
    match event {
        SwarmEvent::NewListenAddr { address, .. } => {
            log::info!("listening on {address}");
        }
        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
            log::info!("connected to {peer_id}");
            let _ = swarm
                .behaviour_mut()
                .sync
                .send_request(&peer_id, SyncRequest::Status);
        }
        SwarmEvent::ConnectionClosed { peer_id, .. } => {
            log::info!("disconnected from {peer_id}");
        }

        // ---- gossipsub ----
        SwarmEvent::Behaviour(IonicBehaviourEvent::Gossipsub(
            gossipsub::Event::Message { message, propagation_source, .. }
        )) => {
            if message.topic == block_topic.hash() {
                match decode_block(&message.data) {
                    Ok(block) => {
                        let hash = block.hash();
                        match node.accept_block(block) {
                            Ok(()) => log::info!(
                                "accepted block {hash} from {propagation_source}"
                            ),
                            Err(e) => log::warn!("rejected block {hash}: {e}"),
                        }
                    }
                    Err(e) => log::warn!("block decode error from {propagation_source}: {e}"),
                }
            } else if message.topic == tx_topic.hash() {
                match decode_transaction(&message.data) {
                    Ok(tx) => {
                        let h = tx.hash();
                        match node.accept_transaction(&tx).await {
                            Ok(())  => log::info!("accepted tx {h} from {propagation_source}"),
                            Err(e)  => log::debug!("rejected tx {h}: {e}"),
                        }
                    }
                    Err(e) => log::warn!("tx decode error from {propagation_source}: {e}"),
                }
            }
        }

        // ---- sync protocol ----
        SwarmEvent::Behaviour(IonicBehaviourEvent::Sync(
            request_response::Event::Message { peer, message, .. }
        )) => match message {
            request_response::Message::Request { request, channel, .. } => {
                let response = match request {
                    SyncRequest::Status => SyncResponse::Status {
                        chain_id: node.chain_id,
                        height:   node.head_height(),
                        head:     node.head_hash(),
                    },
                    SyncRequest::BlockByIndex(i) => SyncResponse::Block(
                        node.chain.get_block(i).cloned(),
                    ),
                    SyncRequest::BlockByHash(h) => SyncResponse::Block(
                        node.get_block_by_hash(&h).cloned(),
                    ),
                };
                let _ = swarm.behaviour_mut().sync.send_response(channel, response);
            }
            request_response::Message::Response { response, .. } => match response {
                SyncResponse::Status { chain_id, height, head } => {
                    log::info!("peer {peer} status: chain={chain_id} h={height} head={head}");
                    let mut cursor = node.head_height();
                    while cursor < height {
                        cursor += 1;
                        let _ = swarm.behaviour_mut().sync.send_request(
                            &peer, SyncRequest::BlockByIndex(cursor),
                        );
                    }
                }
                SyncResponse::Block(Some(block)) => {
                    let hash = block.hash();
                    match node.accept_block(block) {
                        Ok(())  => log::info!("synced block {hash} from {peer}"),
                        Err(e)  => log::warn!("sync block {hash} invalid: {e}"),
                    }
                }
                SyncResponse::Block(None) => log::debug!("peer {peer} has no such block"),
            },
        },

        // ---- kad / identify / ping: log and continue ----
        SwarmEvent::Behaviour(IonicBehaviourEvent::Kademlia(ev)) => {
            log::debug!("kad: {ev:?}");
        }
        SwarmEvent::Behaviour(IonicBehaviourEvent::Identify(ev)) => {
            log::debug!("identify: {ev:?}");
        }
        _ => {}
    }
    Ok(())
}

// publish helper kept for future RPC surface
#[allow(dead_code)]
fn broadcast_tx(
    swarm: &mut Swarm<IonicBehaviour>,
    topic: &gossipsub::IdentTopic,
    tx: &ionic::transactions::Transaction,
) -> Result<()> {
    swarm
        .behaviour_mut()
        .gossipsub
        .publish(topic.clone(), encode_transaction(tx))
        .map_err(|e| anyhow!("gossip publish: {e}"))?;
    Ok(())
}
