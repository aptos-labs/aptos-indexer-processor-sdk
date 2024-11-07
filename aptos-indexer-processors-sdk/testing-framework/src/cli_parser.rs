use once_cell::sync::Lazy;
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub struct TestArgs {
    pub generate_output: bool,
    pub output_path: Option<String>,
}

// Define a global static to store the parsed arguments
static TEST_CONFIG: Lazy<Mutex<TestArgs>> = Lazy::new(|| {
    let args = parse_test_args();
    Mutex::new(args)
});

// Function to fetch global test args
pub fn get_test_config() -> (bool, Option<String>) {
    let test_args = TEST_CONFIG.lock().unwrap().clone();
    (test_args.generate_output, test_args.output_path)
}

pub fn parse_test_args() -> TestArgs {
    let raw_args: Vec<String> = std::env::args().collect();

    // Find the position of the "--" separator to identify custom arguments
    let custom_args_start = raw_args
        .iter()
        .position(|arg| arg == "--")
        .map_or(0, |pos| pos + 1);

    // Collect custom arguments based on the determined start position
    let custom_args: Vec<String> = raw_args[custom_args_start..]
        .iter()
        .filter(|&arg| arg != "--nocapture") // Ignore standard flags like --nocapture
        .cloned()
        .collect();

    // Manually parse the "generate-output" flag
    let generate_output_flag = custom_args.contains(&"generate-output".to_string());

    // Manually parse the "output-path" flag and get its associated value
    let output_path = custom_args
        .windows(2)
        .find(|args| args[0] == "output-path")
        .map(|args| args[1].clone());

    // Validate that --output-path is not provided without --generate-output
    if output_path.is_some() && !generate_output_flag {
        panic!("Error: output-path cannot be provided without generate-output.");
    }

    TestArgs {
        generate_output: generate_output_flag,
        output_path,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub fn parse_test_args_from_vec(args: Vec<String>) -> TestArgs {
        let custom_args_start = args
            .iter()
            .position(|arg| arg == "--")
            .map_or(0, |pos| pos + 1);

        let custom_args: Vec<String> = args[custom_args_start..]
            .iter()
            .filter(|&arg| arg != "--nocapture") // Ignore standard flags like --nocapture
            .cloned()
            .collect();

        let generate_output_flag = custom_args.contains(&"generate-output".to_string());
        let output_path = custom_args
            .windows(2)
            .find(|args| args[0] == "output-path")
            .map(|args| args[1].clone());

        if output_path.is_some() && !generate_output_flag {
            panic!("Error: output-path cannot be provided without generate-output.");
        }

        TestArgs {
            generate_output: generate_output_flag,
            output_path,
        }
    }

    #[test]
    fn test_parse_generate_output_flag() {
        let args = vec!["--".to_string(), "generate-output".to_string()];
        let parsed = parse_test_args_from_vec(args);
        assert!(parsed.generate_output);
        assert_eq!(parsed.output_path, None);
    }

    #[test]
    fn test_parse_generate_output_flag_with_binary() {
        let args = vec![
            "test_binary".to_string(),
            "--".to_string(),
            "generate-output".to_string(),
        ];
        let parsed = parse_test_args_from_vec(args);
        assert!(parsed.generate_output);
        assert_eq!(parsed.output_path, None);
    }

    #[test]
    fn test_parse_both_arguments() {
        let args = vec![
            "test_binary".to_string(),
            "--".to_string(),
            "generate-output".to_string(),
            "output-path".to_string(),
            "/some/other/path".to_string(),
        ];
        let parsed = parse_test_args_from_vec(args);
        assert!(parsed.generate_output);
        assert_eq!(parsed.output_path, Some("/some/other/path".to_string()));
    }

    #[test]
    fn test_parse_no_arguments() {
        let args = vec!["test_binary".to_string()];
        let parsed = parse_test_args_from_vec(args);
        assert!(!parsed.generate_output);
        assert_eq!(parsed.output_path, None);
    }

    #[test]
    #[should_panic(expected = "Error: output-path cannot be provided without generate-output.")]
    fn test_output_path_without_generate_output() {
        let args = vec![
            "test_binary".to_string(),
            "--".to_string(),
            "output-path".to_string(),
            "/some/path".to_string(),
        ];
        parse_test_args_from_vec(args);
    }

    #[test]
    fn test_ignore_nocapture_flag() {
        let args = vec![
            "test_binary".to_string(),
            "--".to_string(),
            "--nocapture".to_string(),
            "generate-output".to_string(),
        ];
        let parsed = parse_test_args_from_vec(args);
        assert!(parsed.generate_output);
        assert_eq!(parsed.output_path, None);
    }
}
