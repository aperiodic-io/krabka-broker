use {super::*, assert2::assert, object_store::path::Path};

#[tokio::test]
async fn mock_seam_compiles_and_returns() {
    let mut mock = MockObjectOps::new();
    mock.expect_get()
        .returning(|_| Ok(bytes::Bytes::from_static(b"x")));
    mock.expect_put().returning(|_, bytes, _| {
        Ok(PutOutcome {
            size_bytes: bytes.len() as u64,
            sha256: None,
            e_tag: None,
            version_id: None,
            create_precondition: false,
        })
    });

    let got = mock.get(&Path::from("k")).await.unwrap();
    let outcome = mock
        .put(
            &Path::from("k"),
            bytes::Bytes::from_static(b"xy"),
            PutRequest::default(),
        )
        .await
        .unwrap();

    assert!(&got[..] == b"x");
    assert!(
        outcome
            == PutOutcome {
                size_bytes: 2,
                sha256: None,
                e_tag: None,
                version_id: None,
                create_precondition: false,
            }
    );
}
