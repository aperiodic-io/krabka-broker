//! Tests for the reassignment background task in [`super`]: it submits a ready
//! reassignment when the image changes, and it stays quiet when this node is
//! not the controller leader.
//!
//! The tests for the per-partition decision live beside that decision in
//! [`super::policy`].

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use assert2::assert;
use async_trait::async_trait;
use krabka_metadata::{MetadataImage, MetadataRecord};
use krabka_raft::NodeId;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::{
    reassignment::test_support::{first_partition, img, liveness},
    test_support::ReassignmentSetup,
};

struct MockReassignmentController {
    is_leader: AtomicBool,
    current: Mutex<Arc<MetadataImage>>,
    image_tx: watch::Sender<Arc<MetadataImage>>,
    submitted: Mutex<Vec<Vec<MetadataRecord>>>,
}

impl MockReassignmentController {
    fn new(is_leader: bool, image: Arc<MetadataImage>) -> Self {
        let (image_tx, _) = watch::channel(image.clone());
        Self {
            is_leader: AtomicBool::new(is_leader),
            current: Mutex::new(image),
            image_tx,
            submitted: Mutex::new(Vec::new()),
        }
    }

    fn publish(&self, image: Arc<MetadataImage>) {
        *self.current.lock().expect("current image mutex poisoned") = image.clone();
        self.image_tx
            .send(image)
            .expect("run loop is watching image");
    }

    fn submitted_len(&self) -> usize {
        self.submitted
            .lock()
            .expect("submitted mutex poisoned")
            .len()
    }

    fn submissions(&self) -> Vec<Vec<MetadataRecord>> {
        self.submitted
            .lock()
            .expect("submitted mutex poisoned")
            .clone()
    }
}

#[async_trait]
impl ReassignmentController for MockReassignmentController {
    fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::SeqCst)
    }

    fn current_image(&self) -> Arc<MetadataImage> {
        self.current
            .lock()
            .expect("current image mutex poisoned")
            .clone()
    }

    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        self.image_tx.subscribe()
    }

    async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), String> {
        self.submitted
            .lock()
            .expect("submitted mutex poisoned")
            .push(records);
        Ok(())
    }
}

async fn wait_for_submission_count(controller: &MockReassignmentController, count: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if controller.submitted_len() >= count {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reassignment run loop did not submit expected records");
}

async fn start_watching_reassignment(
    is_leader: bool,
) -> (
    Arc<MockReassignmentController>,
    CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    let initial = img(ReassignmentSetup {
        replicas: vec![krabka_ids::NodeId(1)],
        isr: vec![krabka_ids::NodeId(1)],
        ..Default::default()
    });
    let controller = Arc::new(MockReassignmentController::new(is_leader, initial));
    let l = Arc::new(liveness(&[1, 2, 3]).await);
    let shutdown = CancellationToken::new();
    let task_controller: Arc<dyn ReassignmentController> = controller.clone();
    let task = tokio::spawn(run(task_controller, l, shutdown.clone()));
    tokio::task::yield_now().await;
    controller.publish(img(ReassignmentSetup {
        adding: vec![krabka_ids::NodeId(3)],
        removing: vec![krabka_ids::NodeId(2)],
        ..Default::default()
    }));
    (controller, shutdown, task)
}

#[tokio::test]
async fn run_submits_ready_reassignment_on_image_change() {
    let (controller, shutdown, task) = start_watching_reassignment(true).await;
    wait_for_submission_count(&controller, 1).await;

    shutdown.cancel();
    task.await.expect("reassignment task panicked");
    let submissions = controller.submissions();
    assert!(submissions.len() == 1);
    assert!(submissions[0].len() == 1);
    let pr = first_partition(&submissions[0][0]);
    assert!(pr.replicas == vec![NodeId(1), NodeId(3)]);
    assert!(pr.partition_epoch == 1);
}

#[tokio::test]
async fn run_skips_ready_reassignment_when_not_leader() {
    let (controller, shutdown, task) = start_watching_reassignment(false).await;
    let observed = tokio::time::timeout(
        Duration::from_millis(100),
        wait_for_submission_count(&controller, 1),
    )
    .await;

    shutdown.cancel();
    task.await.expect("reassignment task panicked");
    assert!(observed.is_err(), "non-leader must not submit changes");
    assert!(controller.submitted_len() == 0);
}
