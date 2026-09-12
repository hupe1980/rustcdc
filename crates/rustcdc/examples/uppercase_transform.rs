use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn init(_config_json: &str) -> i32 {
    0
}

#[wasm_bindgen]
pub fn transform(event_json: &str) -> String {
    event_json.to_uppercase()
}

#[wasm_bindgen]
pub fn shutdown() -> i32 {
    0
}

fn main() {
    // Example entry point for host builds; exported functions are used by WASM hosts.
}
