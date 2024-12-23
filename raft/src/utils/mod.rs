use rand_core::RngCore;

pub fn random_election_countdown(
    rng: &mut impl RngCore,
    min_ticks_to_election: u32,
    max_ticks_to_election: u32,
) -> u32 {
    let random = rng
        .next_u32()
        .checked_rem(max_ticks_to_election - min_ticks_to_election)
        .unwrap_or(0);
    min_ticks_to_election.saturating_add(random)
}
