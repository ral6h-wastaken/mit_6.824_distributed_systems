use std::{env, io, thread, time};

fn main() -> io::Result<()> {
    let args = env::args();

    if args.len() < 2 {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            String::from("Usage: mrcoordinator inputfiles..."),
        ));
    }

    let coord = mr::coordinator::Coordinator::new(args.skip(1).collect(), 10);

    while !coord.done() {
        thread::sleep(time::Duration::from_secs(1_u64));
    }

    thread::sleep(time::Duration::from_secs(1_u64));
    Ok(())
}
