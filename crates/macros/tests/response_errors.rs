//! Generated error helpers preserve the protocol type's other fields.

use assert2::assert;

trait ErrorResponse: Default {
    fn error(error_code: i16, error_message: Option<String>) -> Self;
}

trait ErrorCodeResponse: Default {
    fn error(error_code: i16) -> Self;
}

trait ErrorRow {
    fn error_code(&self) -> i16;
    fn set_error(&mut self, error_code: i16, error_message: Option<String>);
}

trait ResourceErrorRow: ErrorRow + Default {
    fn for_resource(resource_type: i8, resource_name: String) -> Self;
}

#[derive(Default)]
struct Response {
    error_code: i16,
    error_message: Option<String>,
    throttle_time_ms: i32,
}

mod protocol {
    #[derive(Default)]
    pub struct CodeResponse {
        pub error_code: i16,
        pub throttle_time_ms: i32,
    }
}

#[derive(Default)]
struct Row {
    error_code: i16,
    error_message: Option<String>,
    partition_index: i32,
}

#[derive(krabka_macros::FieldDefaults)]
struct ResourceRow {
    error_code: i16,
    #[default(Some("default message".to_owned()))]
    error_message: Option<String>,
    resource_type: i8,
    resource_name: String,
    #[default(17)]
    retained: i32,
}

krabka_macros::response_errors! {
    response {
        Response
    }
    code_response {
        protocol::CodeResponse,
    }
    row {
        Row
    }
    resource_row {
        ResourceRow
    }
}

#[test]
fn builds_response_errors_with_other_fields_at_default() {
    let response = Response::error(41, Some("refused".to_owned()));
    assert!(response.error_code == 41);
    assert!(response.error_message.as_deref() == Some("refused"));
    assert!(response.throttle_time_ms == 0);
    assert!(Response::error(42, None).error_message == None);

    let code_response = protocol::CodeResponse::error(43);
    assert!(code_response.error_code == 43);
    assert!(code_response.throttle_time_ms == 0);
}

#[test]
fn row_errors_preserve_identifiers_and_can_clear_messages() {
    let mut row = Row {
        partition_index: 7,
        ..Default::default()
    };
    row.set_error(41, Some("refused".to_owned()));
    assert!(row.error_code() == 41);
    assert!(row.error_message.as_deref() == Some("refused"));
    assert!(row.partition_index == 7);
    row.set_error(0, None);
    assert!(row.error_code() == 0);
    assert!(row.error_message == None);
}

#[test]
fn resource_rows_also_implement_error_row_and_clear_default_messages() {
    let mut row = ResourceRow::for_resource(2, "orders".to_owned());
    assert!(row.resource_type == 2);
    assert!(row.resource_name == "orders");
    assert!(row.error_code() == 0);
    assert!(row.error_message == None);
    assert!(row.retained == 17);
    row.set_error(41, Some("refused".to_owned()));
    assert!(row.error_code() == 41);
    assert!(row.error_message.as_deref() == Some("refused"));
    assert!(row.resource_name == "orders");
}
