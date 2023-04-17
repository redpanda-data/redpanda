use std::env;
use std::fs::OpenOptions;
use std::io::BufReader;

mod deltafor;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        panic!("Not enough arguments");
    }
    let mode = &args[1];
    let fname = &args[2];
    if mode != "segment-index" {
        panic!("Mode {} is not supported", mode);
    }
    let file = OpenOptions::new().read(true).open(fname).unwrap();
    let mut stream = BufReader::new(file);
    let dec = deltafor::read_index(&mut stream).unwrap();
    println!("{}", serde_json::to_string(&dec).unwrap());
}
