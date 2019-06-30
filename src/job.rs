use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Job<T> {
  pub id: String,
  pub status: String,
  #[serde(bound(serialize = "T: Serialize", deserialize = "T: Deserialize<'de>"))]
  pub data: T,
  pub perform_at: DateTime<Utc>,
  pub tries: u32,
  pub max_tries: u32,
}

impl<T> Job<T> {
  pub fn new(data: T) -> Self {
    Job {
      id: Uuid::new_v4().to_string(),
      status: String::from("draft"),
      data,
      perform_at: Utc::now(),
      tries: 0,
      max_tries: 10,
    }
  }

  pub fn perform_in(&mut self, duration: chrono::Duration) {
    self.perform_at = self.perform_at + duration;
  }

  pub fn retry(&mut self) {
    if self.tries == self.max_tries {
      self.change_status("dead");
    } else {
      self.change_status("scheduled");
      self.perform_at = Utc::now();
      self.tries = self.tries + 1;
      self.perform_in(Duration::seconds(2_i64.pow(self.tries)))
    }
  }

  pub fn change_status(&mut self, status: &str) {
    self.status = String::from(status);
  }
}

#[cfg(test)]
mod tests {
  use super::Job;
  use chrono::Utc;

  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  struct Foo {}

  #[test]
  fn it_creates_jobs_with_default_options() {
    let job = Job::new(Foo {});
    assert_eq!(job.data, Foo {});
    assert_eq!(job.tries, 0);
    assert_eq!(job.max_tries, 10);
    assert!(job.perform_at < Utc::now());
    assert_eq!(&job.status, "draft");
  }

  #[test]
  fn it_dies_once_it_has_reached_the_number_of_max_tries() {
    let mut job = Job::new(Foo {});

    for try_number in 0..10 {
      let previous_perform_at = job.perform_at.clone();
      job.retry();
      assert_eq!(job.tries, try_number + 1);
      assert_eq!(&job.status, "scheduled");
      assert!(job.perform_at > previous_perform_at);
    }

    job.retry();
    assert_eq!(&job.status, "dead");
    assert_eq!(job.tries, job.max_tries);
  }

  #[test]
  fn it_can_marked_as_running() {
    let mut job = Job::new(Foo {});
    job.change_status("running");
    assert_eq!(&job.status, "running");
  }
}
