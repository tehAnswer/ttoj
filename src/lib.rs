extern crate chrono;
extern crate redis;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate uuid;

pub mod job;
pub mod jobs_center;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
