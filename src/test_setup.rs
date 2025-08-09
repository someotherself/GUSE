#[derive(Debug, Clone)]
pub struct TestSetup {
    #[allow(dead_code)]
    pub key: &'static str,
    pub read_only: bool,
}
