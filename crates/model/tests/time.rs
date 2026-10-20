//! A Time from `model::time::now` reads back from the API unchanged.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;

#[test]
fn now_survives_a_serde_round_trip() {
    let t = model::time::now();
    let json = serde_json::to_string(&t).expect("serialize");
    let back: Time = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back, t);
    assert_eq!(t.0.subsec_nanosecond(), 0);
}
