//! Capacity control (design §6.4). v0 ships a fixed capacity; the hook exists
//! so an adaptive controller can be added without changing the set. Any
//! controller must decide from consensus-visible statistics only.

/// Statistics a controller may use. All are derived from committed state.
#[derive(Clone, Copy, Debug, Default)]
pub struct SetStats {
    pub occupancy: usize,
    pub last_batch_size: usize,
    pub last_eviction_size: usize,
    pub total_evictions: u64,
}

pub trait CapacityController: Send + Sync + 'static {
    /// Target capacity after the commit at `height`.
    fn target(&mut self, stats: &SetStats, height: u64) -> usize;
}

/// Constant capacity; the v0 default, seeded from `Config.capacity`.
pub struct Fixed(pub usize);

impl CapacityController for Fixed {
    fn target(&mut self, stats: &SetStats, height: u64) -> usize {
        self.0
    }
}
