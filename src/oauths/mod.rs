pub mod google;

pub use google::*;

pub trait OAuthProvider {
    fn get_provider(&self) -> &'static str;
}
