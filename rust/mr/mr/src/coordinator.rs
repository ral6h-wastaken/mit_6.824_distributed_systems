pub struct Coordinator {}

impl Coordinator {
    pub fn new(input_files: Vec<String>, n_reduce: i32) -> Self {
        println!("Starting coordinator with input files {input_files:?} and n_reduce {n_reduce}");
        Self{}
    }

    pub fn done(&self) -> bool {
        false
    }
}

