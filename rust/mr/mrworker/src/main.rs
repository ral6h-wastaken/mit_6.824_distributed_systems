use std::{
    env,
    io::{self, ErrorKind},
};

mod workflows {
    pub fn wc_map(filename: String, contents: String) -> Vec<(String, String)> {
        let _ = filename; // ignored, as in the Go version
        contents
            .split(|c: char| !c.is_alphabetic())
            .filter(|w| !w.is_empty())
            .map(|w| (w.to_string(), "1".to_string()))
            .collect()
    }

    pub fn wc_reduce(key: String, values: Vec<String>) -> String {
        let _ = key;
        values.len().to_string()
    }
}

fn main() -> io::Result<()> {
    let args = env::args();

    if args.len() != 2 {
        return Err(io::Error::new(
            ErrorKind::Other,
            String::from("Usage: mrworker <workflow>"),
        ));
    }

    let (mapf, reducef) = load_workflow(args.collect::<Vec<String>>().get(1).unwrap());

    mr::worker::run(mapf, reducef)
}

fn load_workflow(
    workflow: &str,
) -> (
    fn(String, String) -> Vec<(String, String)>,
    fn(String, Vec<String>) -> String,
) {
    match workflow {
        "wc.so" => (workflows::wc_map, workflows::wc_reduce),
        _ => panic!("unknown workflow: {}", workflow),
    }
}
