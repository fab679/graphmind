//! Property Indexing module
//!
//! Provides B-Tree indices for optimizing property lookups.

pub mod manager;
pub mod property_index;

pub use manager::{IndexManager, PropertyIndexKey};
pub use property_index::PropertyIndex;
