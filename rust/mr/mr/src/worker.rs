use std::io;

pub fn run(
    mapf: fn(String, String) -> Vec<(String, String)>,
    reducef: fn(String, Vec<String>) -> String,
) -> io::Result<()> {
    //TODO [LS]: complete
    loop {}
}

