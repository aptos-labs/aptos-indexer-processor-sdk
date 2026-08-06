use url::Url;

pub fn redact_string(url: &str) -> String {
    let mut parsed = match Url::parse(url) {
        Ok(u) => u,
        Err(_) => return "[unparseable connection string]".to_string(),
    };

    // Redact password in the authority (user:pass@host) section.
    if parsed.password().is_some() && parsed.set_password(Some("REDACTED")).is_err() {
        // set_password fails only when the URL has no host, which cannot
        // happen when password() returned Some — but return a safe fallback
        // rather than leak credentials.
        return "[connection string]".to_string();
    }

    // Redact password supplied as a query parameter (e.g. ?password=secret).
    let has_password_param = parsed.query_pairs().any(|(k, _)| k == "password");
    if has_password_param {
        let new_pairs: Vec<(String, String)> = parsed
            .query_pairs()
            .map(|(k, v)| {
                if k.as_ref() == "password" {
                    (k.into_owned(), "REDACTED".to_string())
                } else {
                    (k.into_owned(), v.into_owned())
                }
            })
            .collect();
        {
            let mut qpm = parsed.query_pairs_mut();
            qpm.clear();
            for (k, v) in &new_pairs {
                qpm.append_pair(k, v);
            }
        }
    }

    parsed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redact_authority_password() {
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

    #[test]
    fn test_redact_query_param_password() {
        assert_eq!(
            redact_string("postgresql://user@localhost:5432/mydb?password=secret"),
            "postgresql://user@localhost:5432/mydb?password=REDACTED"
        );
    }

    #[test]
    fn test_redact_query_param_password_only() {
        assert_eq!(
            redact_string("postgresql:///mydb?user=u&password=secret"),
            "postgresql:///mydb?user=u&password=REDACTED"
        );
    }
}
