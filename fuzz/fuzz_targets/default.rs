#![no_main]
use libfuzzer_sys::fuzz_target;
use tsdb::chunks::common::read_varint_i64;

fuzz_target!(|data: &[u8]| {
    let _ = read_varint_i64(data, 0);
});
