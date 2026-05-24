use regex::Regex;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn init(_config_json: &str) -> i32 {
    0
}

#[wasm_bindgen]
pub fn transform(event_json: &str) -> String {
    let matcher = match Regex::new("\"table\"\\s*:\\s*\"users\"") {
        Ok(regex) => regex,
        Err(_) => return String::new(),
    };

    if matcher.is_match(event_json) {
        event_json.to_string()
    } else {
        String::new()
    }
}

#[wasm_bindgen]
pub fn shutdown() -> i32 {
    0
}

fn main() {
    // Example entry point for host builds; exported functions are used by WASM hosts.
}
