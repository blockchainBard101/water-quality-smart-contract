module fms::fms;

use std::string::String;
use sui::clock::Clock;
use sui::table::{Self, Table};
use sui::event;

/// Fixed-point scale for decimals (e.g., 27.53°C => 2753)
const SCALE: u64 = 100;
const MS_PER_MIN: u64 = 60_000;
const MS_PER_DAY: u64 = 86_400_000;
const MINUTES_PER_DAY: u64 = 1_440;

/// Events for indexing
public struct MinuteReadingUpserted has copy, drop, store {
    device_id: ID,
    day_utc: u64,          // days since Unix epoch (UTC): floor(ts_ms / MS_PER_DAY)
    minute_index: u16,     // [0..1439]
    timestamp_ms: u64,     // recorded timestamp (rounded down to the minute)
    temperature_x100: u64,
    dissolved_oxygen_x100: u64,
    ph_x100: u64,
    turbidity_x100: u64,
    by: address,
}

/// A single sensor reading (fixed-point x100)
public struct SensorReading has copy, drop, store {
    timestamp_ms: u64,             // rounded down to the minute
    temperature_x100: u64,
    dissolved_oxygen_x100: u64,
    ph_x100: u64,
    turbidity_x100: u64,
    present: bool,                 // whether a value exists for this minute
}

/// Per-day bucket: exactly 1440 slots (0..1439). Sparse until filled.
public struct DayBucket has key, store {
    id: UID,
    day_utc: u64,                  
    readings: vector<SensorReading>,   // length = 1440
    filled: u32,                   // number of minutes written (for stats)
}
 
/// Device → owns a table of (day_utc → DayBucket)
public struct Device has key, store {
    id: UID,
    name: String,
    owner: address,
    days: Table<u64, DayBucket>,
    first_day_utc: Option<u64>,
    last_day_utc: Option<u64>,
    created_ms: u64,
}

/* ─────────────── Setup ─────────────── */

public fun create_device(name: String, clock: &Clock, ctx: &mut TxContext) {
    let now = clock.timestamp_ms();
    let d = Device {
        id: object::new(ctx),
        name,
        owner: ctx.sender(),
        days: table::new<u64, DayBucket>(ctx),
        first_day_utc: option::none<u64>(),
        last_day_utc: option::none<u64>(),
        created_ms: now,
    };

    transfer::public_share_object(d);
}

/* ─────────────── Write (1/min enforced) ─────────────── */

/// Submit a reading. If a reading already exists for that minute, it is **overwritten** (idempotent).
/// All inputs are ×100 fixed-point.
public fun submit_reading_x100(
    device: &mut Device,
    temperature_x100: u64,
    dissolved_oxygen_x100: u64,
    ph_x100: u64,
    turbidity_x100: u64,
    clock: &Clock,
    ctx: &mut TxContext
) {
    assert!(device.owner == tx_context::sender(ctx), 0); // keep or replace with a writer-allowlist

    let ts_ms_full = clock.timestamp_ms();
    let ts_ms = ts_ms_full - (ts_ms_full % MS_PER_MIN); // round down to minute
    let day_utc = ts_ms / MS_PER_DAY;
    let minute_idx_u64 = (ts_ms % MS_PER_DAY) / MS_PER_MIN;
    assert!(minute_idx_u64 < MINUTES_PER_DAY, 1);
    let minute_idx = minute_idx_u64 as u16;

    // ensure bucket exists
    let has = table::contains(&device.days, day_utc);
    if (!has) {
        let bucket = new_day_bucket(day_utc, ctx);
        table::add(&mut device.days, day_utc, bucket);
        if (option::is_none(&device.first_day_utc)) {
            device.first_day_utc = option::some<u64>(day_utc);
        }
    };
    device.last_day_utc = option::some<u64>(day_utc);

    // upsert minute slot
    let b_ref = table::borrow_mut(&mut device.days, day_utc);
    let prev = vector::borrow_mut(&mut b_ref.readings, minute_idx as u64);
    let was_present = (*prev).present;

    *prev = SensorReading {
        timestamp_ms: ts_ms,
        temperature_x100,
        dissolved_oxygen_x100,
        ph_x100,
        turbidity_x100,
        present: true,
    };
    if (!was_present) {
        b_ref.filled = b_ref.filled + 1;
    };

    event::emit(MinuteReadingUpserted {
        device_id: object::uid_to_inner(&device.id),
        day_utc,
        minute_index: minute_idx,
        timestamp_ms: ts_ms,
        temperature_x100,
        dissolved_oxygen_x100,
        ph_x100,
        turbidity_x100,
        by: tx_context::sender(ctx),
    });
}

/// Convenience helper to submit using natural units split into whole + 2 decimals.
public fun submit_reading_parts(
    device: &mut Device,
    t_w: u64, t_d2: u64,
    do_w: u64, do_d2: u64,
    ph_w: u64, ph_d2: u64,
    tu_w: u64, tu_d2: u64,
    clock: &Clock,
    ctx: &mut TxContext
) {
    submit_reading_x100(
        device,
        to_x100(t_w, t_d2),
        to_x100(do_w, do_d2),
        to_x100(ph_w, ph_d2),
        to_x100(tu_w, tu_d2),
        clock,
        ctx
    )
}

/* ─────────────── Views / Queries ─────────────── */

/// Get a reading by (day_utc, minute_index [0..1439]). Returns None if that minute is empty.
public fun get_at_minute(device: &Device, day_utc: u64, minute_index: u16): Option<SensorReading> {
    if (!table::contains(&device.days, day_utc)) {
        return option::none<SensorReading>()
    };
    assert!((minute_index as u64) < MINUTES_PER_DAY, 2);
    let b_ref = table::borrow(&device.days, day_utc);
    let slot = vector::borrow(&b_ref.readings, minute_index as u64);
    if (slot.present) option::some<SensorReading>(*slot) else option::none<SensorReading>()
}

/// Get the latest recorded minute if any (searches from last_day_utc backwards if needed).
/// O(1) when today has data; worst-case scans back across days until it finds one.
public fun latest(device: &Device): Option<SensorReading> {
    if (option::is_none(&device.last_day_utc)) return option::none<SensorReading>();
    let mut day = option::borrow(&device.last_day_utc);
    loop {
        if (table::contains(&device.days, *day)) {
            let b_ref = table::borrow(&device.days, *day);
            let mut i = MINUTES_PER_DAY;
            while (i > 0) {
                i = i - 1;
                let slot = vector::borrow(&b_ref.readings, i);
                if (slot.present) return option::some<SensorReading>(*slot)
            };
        };
        if (option::is_none(&device.first_day_utc) || day == option::borrow(&device.first_day_utc)){
            break
        } 
        else {
            let d = *day - 1;
            day = &d
        }
    };
    option::none<SensorReading>()
}

/// Day stats
public fun day_filled_count(device: &Device, day_utc: u64): u32 {
    if (!table::contains(&device.days, day_utc)) return 0;
    let b_ref = table::borrow(&device.days, day_utc);
    b_ref.filled
}

public fun first_day(device: &Device): Option<u64> { device.first_day_utc }
public fun last_day(device: &Device): Option<u64> { device.last_day_utc }
public fun name(device: &Device): &String { &device.name }
public fun owner(device: &Device): address { device.owner }
public fun created_ms(device: &Device): u64 { device.created_ms }

/* ─────────────── Internals ─────────────── */

fun new_day_bucket(day_utc: u64, ctx: &mut TxContext): DayBucket {
    let mut readings = vector::empty<SensorReading>();
    let mut i = 0;
    while (i < MINUTES_PER_DAY) {
        vector::push_back(&mut readings, SensorReading {
            timestamp_ms: 0,
            temperature_x100: 0,
            dissolved_oxygen_x100: 0,
            ph_x100: 0,
            turbidity_x100: 0,
            present: false,
        });
        i = i + 1;
    };
    DayBucket { id: object::new(ctx), day_utc, readings, filled: 0 }
}

fun to_x100(whole: u64, dp2: u64): u64 {
    assert!(dp2 < SCALE, 3);
    whole * SCALE + dp2
}
