use url::Url;

pub fn redact_string(url: &str) -> String {
    let mut parsed = match Url::parse(url) {
        Ok(u) => u,
        Err(_) => return "[unparseable connection string]".to_string(),
    };
    if parsed.password().is_some() {
        parsed.set_password(Some("REDACTED")).ok();
    }
    parsed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redact_password() {
        assert_eq!(
            redact_string("postgres://user:secret@localhost:5432/mydb"),
            "postgres://user:REDACTED@localhost:5432/mydb"
        );
    }

    #[test]
    fn test_no_password() {
        assert_eq!(
            redact_string("postgres://user@localhost:5432/mydb"),
            "postgres://user@localhost:5432/mydb"
        );
    }

    #[test]
    fn test_unparseable() {
        assert_eq!(
            redact_string("not a url"),
            "[unparseable connection string]"
        );
    }
}
