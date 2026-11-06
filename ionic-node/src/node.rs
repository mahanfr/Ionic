use ionic::{
    blockchian::{Blockchain, BlockchainError},
    blocks::Block,
    mempool::Mempool,
    transactions::Transaction,
    types::{IonicHash, IonicPK},
};

pub struct Node {
    pub chain_id: u64,
    pub chain:    Blockchain,
    pub mempool:  Mempool,
}

impl Node {
    pub fn new(chain_id: u64) -> Self {
        let mut chain = Blockchain::new(chain_id);
        // The library pushes `chain_id` into genesis.index (a quirk).
        // Normalise to 0 so `get_block(height)` is unambiguous.
        if let Some(g) = chain.chain.back_mut() {
            g.header.index = 0;
        }
        Self { chain_id, chain, mempool: Mempool::new() }
    }

    pub fn head(&self) -> Option<&Block> { self.chain.head() }
    pub fn head_height(&self) -> u64 {
        self.head().map(|b| b.header.position.height).unwrap_or(0)
    }
    pub fn head_hash(&self) -> IonicHash {
        self.head().map(|b| b.hash()).unwrap_or_default()
    }

    /// Validate against the current tip, then append.
    pub fn accept_block(&mut self, block: Block) -> Result<(), BlockchainError> {
        // Already have it?
        if let Some(existing) = self.chain.get_block(block.header.index) {
            if existing.hash() == block.hash() {
                return Ok(());
            }
        }
        self.chain.verify_block(&block)?;
        // NOTE: state-transition execution lives in `ionic::state`;
        // this node is a relay/validation node so we just store the block.
        self.chain.chain.push_front(block);
        Ok(())
    }

    pub async fn accept_transaction(&self, tx: &Transaction) -> Result<(), String> {
        self.mempool
            .submit_transaction(tx, &self.chain)
            .await
            .map_err(|e| e.to_string())
    }

    pub fn get_block_by_hash(&self, hash: &IonicHash) -> Option<&Block> {
        self.chain.chain.iter().find(|b| &b.hash() == hash)
    }

    /// Tiny block producer so a demo node can keep the chain moving
    /// and let peers validate/propagate blocks.
    pub fn mine_empty(
        &mut self,
        proposer_pk: IonicPK,
        signer: &ed25519_dalek::SigningKey,
    ) -> Option<Block> {
        let parent = self.chain.head()?.clone();
        let parent_hash = parent.header.hash();
        let next_height = parent.header.position.height + 1;

        let mut block = Block::new_unsigned(
            next_height,                                     // header.index
            ionic::types::BlockPos::new(next_height, 0),     // header.position
            self.chain_id,                                   // header.chain_id
            parent_hash,
            proposer_pk,
            IonicHash::default(),                            // state_root (placeholder)
            Vec::new(),
        );
        block.header.gas_limit = 30_000_000;
        block.header.gas_used  = 0;
        block.header.base_fee  = parent.next_base_fee();
        block.sign(signer);
        Some(block)
    }
}
