use std::ops::{Add, AddAssign};

#[derive(Default, Debug, Clone, Copy, Ord, Eq, PartialEq, PartialOrd)]
pub struct TermId(pub u64);
#[derive(Default, Debug, Clone, Copy, Ord, Eq, PartialEq, PartialOrd)]
pub struct NodeId(pub u64);

impl From<u64> for TermId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<u64> for NodeId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl AddAssign<u64> for TermId {
    fn add_assign(&mut self, rhs: u64) {
        self.0 = self.0.checked_add(rhs).unwrap();
    }
}

impl Add<u64> for TermId {
    type Output = TermId;
    fn add(self, rhs: u64) -> Self::Output {
        Self(self.0 + rhs)
    }
}
