//! In-process checkpoint tests: the exclusive lock and unique temp files.
//! These call the library directly, with several threads standing in for
//! overlapping runner processes.

use std::path::Path;
use std::thread;
use std::time::Duration;

use ptyktos_agent_runner::{checkpoint_path, RunnerState};

#[test]
fn two_overlapping_writers_on_one_key_keep_the_checkpoint_valid_json() {
    let dir = tempfile::tempdir().unwrap();
    let path = checkpoint_path(dir.path(), "saga-overlap");

    let writers: Vec<_> = (0..4)
        .map(|t| {
            let path = path.clone();
            thread::spawn(move || {
                for i in 0..40 {
                    // The lock is held from open until this state is dropped,
                    // so each iteration is one load/change/persist critical section.
                    let mut state = RunnerState::open(&path).unwrap();
                    state.review_id = Some(format!("writer-{t}-{i}"));
                    state.persist().unwrap();
                }
            })
        })
        .collect();
    for w in writers {
        w.join().expect("writer thread panicked");
    }

    let bytes = std::fs::read(&path).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .expect("checkpoint must be valid JSON after overlapping writes");
    assert!(value["review_id"].as_str().unwrap().starts_with("writer-"));
    // No temp files left behind.
    let leftovers: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".tmp"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
}

#[test]
fn a_second_opener_waits_for_the_first_and_sees_its_write() {
    // Lost-update check. The first opener holds the lock, signals, sleeps,
    // and only then writes. The second opener is started after the signal.
    // With the lock it must wait and read the first writer's value. Without
    // it, it would read the empty state before the write.
    let dir = tempfile::tempdir().unwrap();
    let path = checkpoint_path(dir.path(), "saga-serial");
    let (tx, rx) = std::sync::mpsc::channel();

    let first_path = path.clone();
    let first = thread::spawn(move || {
        let mut state = RunnerState::open(&first_path).unwrap();
        tx.send(()).unwrap();
        thread::sleep(Duration::from_millis(300));
        state.review_id = Some("first".into());
        state.persist().unwrap();
    });

    rx.recv().unwrap();
    let second = RunnerState::open(&path).unwrap();
    assert_eq!(second.review_id.as_deref(), Some("first"));
    drop(second);
    first.join().unwrap();
}

#[test]
fn checkpoint_file_names_do_not_collide_across_lifecycles() {
    let dir = Path::new("/unused");
    assert_ne!(
        checkpoint_path(dir, "saga-a"),
        checkpoint_path(dir, "saga-b")
    );
    assert_eq!(checkpoint_path(dir, "saga-a").extension().unwrap(), "json");
}
