use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use pulse_wire::{
    DictionaryRegistry, PropValue, ReplayChunk, WireBatch, WireEvent, BATCH_MAGIC, DEFAULT_DICT_ID,
    FLAG_DICT_ZSTD, FLAG_RAW_ZSTD, REPLAY_MAGIC, WIRE_VERSION,
};

struct TrackingAllocator;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // Safety rail so hostile pre-fix `with_capacity(u32::MAX)` cannot OOM-kill the host OS
        if layout.size() > 512 * 1024 * 1024 {
            let cur = LIVE_BYTES.load(Ordering::Relaxed);
            PEAK_BYTES.fetch_max(cur.saturating_add(layout.size()), Ordering::Relaxed);
            return std::ptr::null_mut();
        }
        // SAFETY: delegates directly to System allocator with unmodified layout.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let prev = LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
            PEAK_BYTES.fetch_max(prev + layout.size(), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: delegates directly to System allocator with matching ptr and layout.
        unsafe { System.dealloc(ptr, layout) };
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size > 512 * 1024 * 1024 {
            let cur = LIVE_BYTES.load(Ordering::Relaxed);
            PEAK_BYTES.fetch_max(cur.saturating_add(new_size), Ordering::Relaxed);
            return std::ptr::null_mut();
        }
        // SAFETY: delegates directly to System allocator.
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            if new_size >= layout.size() {
                let diff = new_size - layout.size();
                let prev = LIVE_BYTES.fetch_add(diff, Ordering::Relaxed);
                PEAK_BYTES.fetch_max(prev + diff, Ordering::Relaxed);
            } else {
                LIVE_BYTES.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        new_ptr
    }
}

fn measure_peak_delta<F, R>(f: F) -> (R, usize)
where
    F: FnOnce() -> R,
{
    let base = LIVE_BYTES.load(Ordering::SeqCst);
    PEAK_BYTES.store(base, Ordering::SeqCst);
    let out = f();
    let peak = PEAK_BYTES.load(Ordering::SeqCst);
    (out, peak.saturating_sub(base))
}

#[test]
fn p0_05_zstd_decompression_bomb_rejected_across_all_decoders() {
    // 100 MiB of zeros compresses down to ~3.2 KiB with Zstd.
    let uncompressed_bomb = vec![0u8; 100 * 1024 * 1024];
    let zstd_payload = zstd::stream::encode_all(uncompressed_bomb.as_slice(), 3).unwrap();
    assert!(
        zstd_payload.len() < 64 * 1024,
        "expected tiny zstd bomb, got {} bytes",
        zstd_payload.len()
    );

    // 1. Raw Zstd PLSB batch frame
    let mut plsb_raw = Vec::with_capacity(6 + zstd_payload.len());
    plsb_raw.extend_from_slice(&BATCH_MAGIC);
    plsb_raw.push(WIRE_VERSION);
    plsb_raw.push(FLAG_RAW_ZSTD);
    plsb_raw.extend_from_slice(&zstd_payload);

    let (res_raw, peak_raw) = measure_peak_delta(|| WireBatch::decode_binary(&plsb_raw));
    assert!(
        res_raw.is_err(),
        "PLSB raw zstd bomb should be rejected, got Ok"
    );
    assert!(
        peak_raw <= 72 * 1024 * 1024,
        "PLSB raw zstd bomb exceeded 64 MiB cap (peak delta = {peak_raw})"
    );

    // 2. Dictionary Zstd PLSB batch frame
    let reg = DictionaryRegistry::default();
    let dict = reg.get(DEFAULT_DICT_ID).unwrap();
    let mut enc = zstd::Encoder::with_dictionary(Vec::new(), 3, dict).unwrap();
    std::io::Write::write_all(&mut enc, &uncompressed_bomb).unwrap();
    let dict_compressed = enc.finish().unwrap();

    let mut plsb_dict = Vec::with_capacity(8 + dict_compressed.len());
    plsb_dict.extend_from_slice(&BATCH_MAGIC);
    plsb_dict.push(WIRE_VERSION);
    plsb_dict.push(FLAG_DICT_ZSTD);
    plsb_dict.extend_from_slice(&DEFAULT_DICT_ID.to_le_bytes());
    plsb_dict.extend_from_slice(&dict_compressed);

    let (res_dict, peak_dict) =
        measure_peak_delta(|| WireBatch::decode_binary_with_registry(&plsb_dict, &reg));
    assert!(
        res_dict.is_err(),
        "PLSB dict zstd bomb should be rejected, got Ok"
    );
    assert!(
        peak_dict <= 72 * 1024 * 1024,
        "PLSB dict zstd bomb exceeded 64 MiB cap (peak delta = {peak_dict})"
    );

    // 3. PLSR ReplayChunk frame
    let mut plsr = Vec::with_capacity(10 + zstd_payload.len());
    plsr.extend_from_slice(&REPLAY_MAGIC);
    plsr.push(WIRE_VERSION);
    plsr.push(FLAG_RAW_ZSTD);
    plsr.extend_from_slice(&(zstd_payload.len() as u32).to_le_bytes());
    plsr.extend_from_slice(&zstd_payload);

    let (res_plsr, peak_plsr) = measure_peak_delta(|| ReplayChunk::decode_binary(&plsr));
    assert!(
        res_plsr.is_err(),
        "PLSR zstd bomb should be rejected, got Ok"
    );
    assert!(
        peak_plsr <= 72 * 1024 * 1024,
        "PLSR zstd bomb exceeded 64 MiB cap (peak delta = {peak_plsr})"
    );
}

#[test]
fn p0_06_string_table_amplification_attack_rejected() {
    // 1x 32 KiB string referenced across 4 fields + 4 string props in 3,000 events
    // -> decompressed frame is < 350 KiB, but naive cloning materializes > 768 MiB of Strings!
    let big = "X".repeat(32 * 1024);
    let mut events = Vec::with_capacity(3_000);
    for i in 0..3_000usize {
        events.push(WireEvent {
            event_uuid: [i as u8; 16],
            routing_seed: big.clone(),
            distinct_id: big.clone(),
            session_id: big.clone(),
            event_name: big.clone(),
            client_event_ts_ms: 1_700_000_000_000,
            client_sent_ts_ms: 1_700_000_000_000,
            props: vec![
                (big.clone(), PropValue::Str(big.clone())),
                (big.clone(), PropValue::Str(big.clone())),
            ],
            replay_offset_kb: 0,
        });
    }
    let encoded = WireBatch::new(events).encode_binary(3).unwrap();
    drop(big);

    assert!(
        encoded.len() < 64 * 1024,
        "expected tiny encoded amplification frame, got {}",
        encoded.len()
    );

    let (res, peak_delta) = measure_peak_delta(|| WireBatch::decode_binary(&encoded));
    assert!(
        res.is_err(),
        "expected amplification batch to be rejected before allocating hundreds of MiB"
    );
    assert!(
        peak_delta <= 80 * 1024 * 1024,
        "amplification attack allocated {peak_delta} bytes (> 80 MiB)"
    );
}

#[test]
fn p0_06_untrusted_replay_op_and_node_counts_do_not_overallocate() {
    // Craft a tiny uncompressed body claiming `op_count = 4_000_000` with 0 actual ops following.
    let mut raw = Vec::new();
    // project_id = "p"
    raw.extend_from_slice(&1u32.to_le_bytes());
    raw.push(b'p');
    // session_id = "s"
    raw.extend_from_slice(&1u32.to_le_bytes());
    raw.push(b's');
    // chunk_start_ts_ms
    raw.extend_from_slice(&1000i64.to_le_bytes());
    // bogus op_count = 4_000_000 (would pre-allocate ~224 MiB if unchecked!)
    raw.extend_from_slice(&4_000_000u32.to_le_bytes());

    let compressed = zstd::stream::encode_all(raw.as_slice(), 1).unwrap();
    let mut frame = Vec::with_capacity(10 + compressed.len());
    frame.extend_from_slice(&REPLAY_MAGIC);
    frame.push(WIRE_VERSION);
    frame.push(FLAG_RAW_ZSTD);
    frame.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
    frame.extend_from_slice(&compressed);

    let (res, peak_delta) = measure_peak_delta(|| ReplayChunk::decode_binary(&frame));
    assert!(res.is_err(), "truncated replay frame must error");
    assert!(
        peak_delta < 2 * 1024 * 1024,
        "bogus op_count pre-allocated {peak_delta} bytes from a {}-byte frame!",
        frame.len()
    );
}
