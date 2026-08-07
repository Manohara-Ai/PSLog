use super::*;

#[test]
fn test_resolve_topic_explicit_override() {
    let topic = Some("my-custom-topic".to_string());
    let res = resolve_topic(topic, "any-exec", &[]);
    assert_eq!(res, "my-custom-topic");
}

#[test]
fn test_resolve_topic_from_generic_binary() {
    let res = resolve_topic(None, "./my_bin", &["arg1".to_string()]);
    assert_eq!(res, "./my_bin");
}

#[test]
fn test_resolve_topic_from_python_script() {
    let exec = "python3";
    let args = vec!["server.py".to_string(), "--port".to_string(), "8080".to_string()];
    let res = resolve_topic(None, exec, &args);
    assert_eq!(res, "server.py");
}

#[test]
fn test_resolve_topic_python_no_args() {
    let res = resolve_topic(None, "python", &[]);
    assert_eq!(res, "python");
}